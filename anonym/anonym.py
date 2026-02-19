import argparse
import json
import logging
import copy
import re
import os
import shutil
import sys
import hashlib
import traceback
from pathlib import Path
from datetime import datetime
import pandas as pd
import pydicom
from dotenv import load_dotenv
from pydicom.uid import generate_uid
from tabulate import tabulate
from tqdm import tqdm

load_dotenv()

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

SECRET_PEPPER = os.getenv("PEPPER", "default_pepper_if_env_missing")


def get_base_path():
    if getattr(sys, 'frozen', False):
        executable_path = Path(sys.executable)
        if sys.platform == 'darwin':
            return executable_path.parent.parent.parent.parent
        return executable_path.parent
    return Path(__file__).resolve().parent


def get_internal_path(relative_path):
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        return Path(sys._MEIPASS) / relative_path
    return Path(__file__).resolve().parent / relative_path


class AnonymDICOM:
    def __init__(self, input_source, profile_path, output_folder, remove_private_tags=True, show_actions=False):
        self.input_files = input_source
        self.output_folder = Path(output_folder)
        self.remove_private = remove_private_tags
        self.show_actions = show_actions
        self.uid_map = {}
        self.audit_log = []
        self.rules = self._load_profile(profile_path)
        self.pesel_number = ""

        # --- DIRECTORY SCOPE ---
        # Generated ONCE per execution.
        self.batch_study_uid = generate_uid()

        # Placeholder for Series UID (will be set in the loop)
        self.batch_series_uid = None

    def _load_profile(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            rules = json.load(f)
            if self.show_actions:
                print(f"Iterating through tags in config file ({path}):")
                for tag in rules:
                    print(f"  Tag: {tag}")
            return rules

    def _generate_hashed_id(self, original_id):
        combined_string = str(original_id) + SECRET_PEPPER
        return hashlib.sha256(combined_string.encode('utf-8')).hexdigest()

    def _generate_consistent_uid(self, original_uid):
        if original_uid not in self.uid_map:
            self.uid_map[original_uid] = generate_uid()
        return self.uid_map[original_uid]

    def _get_replacement_value(self, vr, action):
        if action == 'D': return ""
        static_defaults = {
            'DA': "", 'TM': "", 'DT': "",
            'PN': "ANONYMIZED", 'AS': "000Y", 'CS': "U", 'DS': "0",
            'IS': "0", 'LO': "ANONYMIZED", 'SH': "ANONYMIZED",
            'ST': "ANONYMIZED", 'LT': "ANONYMIZED", 'UT': "ANONYMIZED", 'AE': "ANONYMIZED",
        }
        return generate_uid() if vr == 'UI' else static_defaults.get(vr, "ANONYMIZED")

    def _process_dataset_recursive(self, dataset):
        if self.remove_private:
            try:
                dataset.remove_private_tags()
            except:
                pass

        for elem in list(dataset):
            keyword = elem.keyword
            if not keyword: continue

            # --- MANDATORY OVERRIDES ---
            # 1. Force the Directory-wide Study UID
            if keyword == "StudyInstanceUID":
                elem.value = self.batch_study_uid
                continue

            # 2. Force the current Series UID
            if keyword == "SeriesInstanceUID":
                if self.batch_series_uid:
                    elem.value = self.batch_series_uid
                continue

            # --- SEQUENCE HANDLING ---
            if elem.VR == 'SQ':
                if keyword in self.rules and self.rules[keyword] == 'X':
                    delattr(dataset, keyword)
                else:
                    for item in elem.value: self._process_dataset_recursive(item)
                continue

            # --- STANDARD PROFILE RULES ---
            if keyword in self.rules:
                action = self.rules[keyword]
                if self.show_actions:
                    print(f"Processing tag from config: {keyword} (Action: {action})")

                if action == 'X':
                    delattr(dataset, keyword)
                elif action == 'U':
                    if elem.value:
                        if elem.VM > 1:
                            elem.value = [self._generate_consistent_uid(u) for u in elem.value]
                        else:
                            elem.value = self._generate_consistent_uid(elem.value)
                elif action in ['Z', 'D']:
                    elem.value = self._get_replacement_value(elem.VR, action)
        return dataset

    def _finalize_metadata(self, dataset, index):
        raw_id = dataset.get("PatientID", "UNKNOWN_PATIENT")
        hashed_id = self._generate_hashed_id(self.pesel_number)
        dataset.PatientID = hashed_id
        dataset.PatientName = f"{hashed_id[:8]}^Anonym"

        today = datetime.now().strftime('%Y%m%d')
        dataset.StudyDate = dataset.SeriesDate = dataset.ContentDate = today
        dataset.InstanceNumber = str(index)
        dataset.PatientIdentityRemoved = "YES"
        dataset.DeidentificationMethod = "DICOM PS3.15 Basic Profile + SHA256"

    def _compare_files_internal(self, ds_orig, ds_anon, filename):
        diffs = []
        self._compare_recursive(ds_orig, ds_anon, diffs, path="ROOT")
        for item in diffs:
            item['File'] = filename
            self.audit_log.append(item)
        return len([d for d in diffs if "FAIL" in d['Status']]), diffs

    def _compare_recursive(self, ds_in, ds_out, results, path=""):
        for elem in ds_in:
            keyword = elem.keyword
            if not keyword: continue
            current_path = f"{path}.{keyword}" if path != "ROOT" else keyword
            elem_out = ds_out.get(elem.tag) if ds_out else None
            val_orig, val_anon = str(elem.value), str(elem_out.value) if elem_out else "MISSING"

            if elem.VR == 'SQ':
                if keyword in self.rules and self.rules[keyword] == 'X':
                    status = "OK" if elem_out is None else "FAIL (Should delete)"
                    results.append({"Tag": current_path, "Action": "X (Seq)", "Status": status, "Original": "Seq",
                                    "Anonymized": val_anon})
                elif elem_out and elem.value:
                    for i, item_orig in enumerate(elem.value):
                        if i < len(elem_out.value):
                            self._compare_recursive(item_orig, elem_out.value[i], results, f"{current_path}[{i}]")
            elif keyword in self.rules:
                action, status = self.rules[keyword], "OK"
                if keyword in ["StudyInstanceUID", "SeriesInstanceUID"]:
                    status = "OK"
                elif action == 'X':
                    status = "FAIL (Leak)" if (elem_out is not None and val_anon != "") else "OK"
                elif action == 'D':
                    status = "FAIL (Not Empty)" if val_anon != "" else "OK"
                elif action == 'Z':
                    status = "FAIL (No Change)" if (
                            str(elem.value) == str(elem_out.value) and str(elem.value) != "") else "OK"
                elif action == 'U':
                    status = "FAIL (UID match)" if str(elem.value) == str(elem_out.value) else "OK"
                results.append({"Tag": current_path, "Action": action, "Status": status, "Original": val_orig,
                                "Anonymized": val_anon})

    def _save_individual_reports(self, diffs, base_filename, args):
        file_id = Path(base_filename).stem

        if args.json_report:
            jd = self.output_folder / "reports_json"
            jd.mkdir(parents=True, exist_ok=True)
            with open(jd / f"{file_id}.json", 'w', encoding='utf-8') as f:
                json.dump(diffs, f, indent=4, ensure_ascii=False)

        if args.html_report:
            hd = self.output_folder / "reports_html"
            hd.mkdir(parents=True, exist_ok=True)
            rows = [[d['Tag'], d['Action'], d['Status'], d['Original'][:40], d['Anonymized'][:40]] for d in diffs]
            table = tabulate(rows, headers=["TAG", "ACT", "STATUS", "ORIGINAL", "ANON"], tablefmt="html")
            with open(hd / f"{file_id}.html", 'w', encoding='utf-8') as f:
                f.write(f"<html><body><h2>Report: {file_id}</h2>{table}</body></html>")

    def _check_comparison(self, dataset):
        dicom_keywords = sorted([elem.keyword for elem in dataset if elem.keyword])
        config_keywords = set(self.rules.keys())

        common = sorted([k for k in dicom_keywords if k in config_keywords])
        only_dicom = sorted([k for k in dicom_keywords if k not in config_keywords])
        only_config = sorted(config_keywords - set(dicom_keywords))

        print("\n" + "="*50)
        print("DICOM TAG vs CONFIG PROFILE COMPARISON")
        print("="*50)
        
        print(f"\nALL TAGS PRESENT IN THIS DICOM FILE ({len(dicom_keywords)}):")
        for k in dicom_keywords:
            print(f"  - {k}")

        print(f"\nMATCHING TAGS (Found in both DICOM and Config - {len(common)}):")
        for k in common:
            print(f"  - {k} (Action: {self.rules[k]})")

        print(f"\nTAGS IN DICOM BUT NOT IN CONFIG ({len(only_dicom)}):")
        for k in only_dicom:
            print(f"  - {k}")

        print("\n" + "="*50)
        print("SUMMARY")
        print(f"Total Tags in File:   {len(dicom_keywords)}")
        print(f"Matching Rules:       {len(common)}")
        print(f"Missing from Config:  {len(only_dicom)}")
        print("="*50 + "\n")

    def run(self, args):
        if not self.output_folder.exists(): self.output_folder.mkdir(parents=True)

        # Prepare files with sorting
        file_data = []
        pattern = re.compile(r"IM-(\d+)-(\d+)\.dcm$", re.IGNORECASE)

        for f in self.input_files:
            # ... (rest of the run method preparation)
            match = pattern.search(f.name)
            if match:
                s_str, i_str = match.groups()
                file_data.append({
                    'path': f,
                    'series_sort': int(s_str),
                    'image_sort': int(i_str),
                    'series_str': s_str,
                    'image_str': i_str,
                    'is_pattern': True
                })
            else:
                file_data.append({
                    'path': f,
                    'series_sort': float('inf'),
                    'image_sort': float('inf'),
                    'series_str': "NON_PATTERN",
                    'image_str': "0000",
                    'is_pattern': False
                })

        # Sort the files so we process them in order (Series 1, Series 2...)
        file_data.sort(key=lambda x: (x['series_sort'], x['image_sort']))

        if args.comparison and file_data:
            first_ds = pydicom.dcmread(file_data[0]['path'])
            self._check_comparison(first_ds)

        current_series_key = None

        # Prepare naming convention
        padding = len(str(len(self.input_files)))
        study_prefix = self.batch_study_uid.split('.')[-1][-6:]
        
        total_files = len(file_data)
        for index, item in enumerate(file_data, 1):
            # Machine-readable progress output
            sys.stderr.write(f"::PROGRESS::{index}/{total_files}::Anonymizing\n")
            sys.stderr.flush()

            file_path = item['path']
            try:
                # --- SERIES UID LOGIC ---
                # Checks if the Series Identifier (XXXX in IM-XXXX-YYYY) has changed
                if item['series_str'] != current_series_key:
                    self.batch_series_uid = generate_uid()
                    current_series_key = item['series_str']
                # ------------------------

                ds = pydicom.dcmread(file_path)
                self.pesel_number = str(ds.get("PatientID", ""))
                ds_orig = copy.deepcopy(ds)

                self._process_dataset_recursive(ds)
                self._finalize_metadata(ds, index)

                if item['is_pattern']:
                    new_filename = f"AN-{item['series_str']}-{item['image_str']}.dcm"
                else:
                    new_filename = f"ST_{study_prefix}_{index:0{padding}}.dcm"

                ds.save_as(self.output_folder / new_filename)

                _, diffs = self._compare_files_internal(ds_orig, ds, new_filename)
                self._save_individual_reports(diffs, new_filename, args)

            except Exception as e:
                logging.error(f"Error processing {file_path}: {e}")

        if self.audit_log and args.summary_report:
            pd.DataFrame(self.audit_log).to_excel(self.output_folder / "SUMMARY_REPORT.xlsx", index=False)


def main():
    try:
        ROOT = get_base_path()
        parser = argparse.ArgumentParser()
        parser.add_argument("--input_dir", type=Path)
        parser.add_argument("--json_report", action="store_true")
        parser.add_argument("--html_report", action="store_true")
        parser.add_argument("--summary_report", action="store_true")
        parser.add_argument("--comparison", action="store_true", help="Show detailed tag comparison between DICOM and config")
        parser.add_argument("--actions", action="store_true", help="Print each tag processing action as it happens")
        args = parser.parse_args()

        if not args.input_dir:
            print("Please provide --input_dir argument")
            return

        INPUT_DIR = args.input_dir
        OUTPUT_DIR = ROOT / "output" / INPUT_DIR.stem

        if OUTPUT_DIR.exists():
            shutil.rmtree(OUTPUT_DIR)

        CONFIG_FILE = ROOT / "config" / "dicom_ps3_15_profile.json"
        if not CONFIG_FILE.exists():
            CONFIG_FILE = get_internal_path("config/dicom_ps3_15_profile.json")

        files = list(INPUT_DIR.glob("*.dcm"))
        if not files:
            print("No files found.")
            return

        anonymizer = AnonymDICOM(files, CONFIG_FILE, OUTPUT_DIR, show_actions=args.actions)
        anonymizer.run(args)
        print(f"Done! Success. Folder: {OUTPUT_DIR}")

    except Exception:
        traceback.print_exc()


if __name__ == "__main__":
    main()
