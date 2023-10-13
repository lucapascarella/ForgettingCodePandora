import argparse
import csv
import os
import pandas as pd
from datetime import datetime
from typing import Dict, Optional
from git import GitCommandError
from pydriller import Repository, Git, ModifiedFile, ModificationType
from tqdm import tqdm
from threading import Lock
from readability import Readability
from process import ProcessMetrics


class GitHubBean:
    def __init__(self, clone_heap: str, owner: str, name: str, sonar_name: str):
        self.checkout = None
        self.heap = clone_heap
        self.owner = owner
        self.name = name
        self.sonar_name = sonar_name
        self.clone_path = os.path.join(clone_heap, owner)
        self.local_path = os.path.join(clone_heap, os.path.join(owner, name))
        self.url = 'https://github.com/' + owner + '/' + name


class Txt:
    def __init__(self, filename: str):
        self.file = open(filename, 'w')

    def write(self, string: str) -> None:
        self.file.write(string)

    def write_and_close(self, string: str) -> None:
        if str is not None:
            self.file.write(string)
        self.file.close()


class MyProgressBar:
    def __init__(self, total: int):
        self.data_lock = Lock()
        self.progress_bar = tqdm(total=total, ascii=True)

    def set_label(self, label: str) -> None:
        self.progress_bar.set_description_str(label)

    def update(self, label: str = None):
        with self.data_lock:
            if label is not None:
                self.set_label(label)
            self.progress_bar.update()

    def close(self):
        self.set_label('Task completed')
        self.progress_bar.close()


def checkout_latest_tag(project: GitHubBean) -> Optional[str]:
    # Checking out
    try:
        checkout_commit = None
        if not os.path.exists(project.clone_path):
            os.makedirs(project.clone_path)
        py_git = Git(os.path.join(project.clone_path, project.name))
        for tag_commit in py_git.get_tagged_commits():
            if checkout_commit is None or py_git.get_commit(tag_commit).author_date > checkout_commit.author_date:
                checkout_commit = py_git.get_commit(tag_commit)

        if checkout_commit is not None:
            py_git.checkout(checkout_commit.hash)

        # Always get head
        head_commit = py_git.get_head()

        return head_commit.hash

    except ValueError as exception:
        print(exception)

        return None


def clone_project(project: GitHubBean) -> bool:
    try:
        # Create destination folder
        if not os.path.exists(project.clone_path):
            os.makedirs(project.clone_path)

        # Force repository to be cloned
        next(Repository(project.url, clone_repo_to=project.clone_path).traverse_commits())

        # Checkout at latest tag commit
        project.checkout = checkout_latest_tag(project)
        return True

    except GitCommandError as exception:
        # processed.append(CloneBean(index, project, exception))
        print(exception)
    except OSError as exception:
        # processed.append(CloneBean(index, project, exception))
        print(exception)
    except ValueError as exception:
        # processed.append(CloneBean(index, project, exception))
        print(exception)

    return False


def get_file_path(mod: ModifiedFile) -> str:
    if mod.change_type == ModificationType.ADD:
        filename = mod.new_path
    elif mod.change_type == ModificationType.COPY:
        filename = mod.new_path
    elif mod.change_type == ModificationType.RENAME:
        filename = mod.new_path
    elif mod.change_type == ModificationType.DELETE:
        filename = mod.old_path
    elif mod.change_type == ModificationType.MODIFY:
        filename = mod.new_path
    else:
        filename = None
    return filename


def main(flags: Dict[str, str]) -> None:
    # Column names: organization, project, analysis_key, date, project_version ,revision, processed, ingested_at
    dfa = pd.read_csv(flags["sonar_analyses_path"], sep=',')

    # Column names: organization, project, current_analysis_key, creation_analysis_key, issue_key, type, rule, severity, status, resolution, effort, debt,
    # tags, creation_date, update_date, close_date, processed, ingested_at
    dfi = pd.read_csv(flags["sonar_issues_path"], sep=',')

    # Column names: organization, project, analysis_key, complexity, class_complexity, function_complexity, file_complexity, function_complexity_distribution,
    # file_complexity_distribution, complexity_in_classes, complexity_in_functions, cognitive_complexity, test_errors, skipped_tests, test_failures, tests,
    # test_execution_time, test_success_density, coverage, lines_to_cover, uncovered_lines, line_coverage, conditions_to_cover, uncovered_conditions,
    # branch_coverage, new_coverage, new_lines_to_cover, new_uncovered_lines, new_line_coverage, new_conditions_to_cover, new_uncovered_conditions,
    # new_branch_coverage, executable_lines_data, public_api, public_documented_api_density, public_undocumented_api, duplicated_lines,
    # duplicated_lines_density, duplicated_blocks, duplicated_files, duplications_data, new_duplicated_lines, new_duplicated_blocks,
    # new_duplicated_lines_density, quality_profiles, quality_gate_details, violations, blocker_violations, critical_violations,
    # major_violations, minor_violations, info_violations, new_violations, new_blocker_violations, new_critical_violations, new_major_violations,
    # new_minor_violations, new_info_violations, false_positive_issues, open_issues, reopened_issues, confirmed_issues, wont_fix_issues, sqale_index,
    # sqale_rating, development_cost, new_technical_debt, sqale_debt_ratio, new_sqale_debt_ratio, code_smells, new_code_smells,
    # effort_to_reach_maintainability_rating_a, new_maintainability_rating, new_development_cost, alert_status, bugs, new_bugs,
    # reliability_remediation_effort, new_reliability_remediation_effort, reliability_rating, new_reliability_rating, last_commit_date,
    # vulnerabilities, new_vulnerabilities, security_remediation_effort, new_security_remediation_effort, security_rating, new_security_rating,
    # security_hotspots, new_security_hotspots, security_review_rating, classes, ncloc, functions, comment_lines, comment_lines_density, files, directories,
    # lines, statements, generated_lines, generated_ncloc, ncloc_data, comment_lines_data, projects, ncloc_language_distribution, new_lines, processed,
    # ingested_at
    dfm = pd.read_csv(flags["sonar_measures_path"], sep=',', low_memory=False)

    # Build a dictionary of dataframes for fast iteration
    df = {"analyses": dfa, "issues": dfi, "measures": dfm}

    # Get basic stats
    print("Projects in analyses {} in issues {} in measures {}".format(dfa.groupby(["organization", 'project']).ngroups,
                                                                       dfi.groupby(["organization", 'project']).ngroups,
                                                                       dfm.groupby(["organization", 'project']).ngroups))

    # Remove empy and NaN columns
    for k, v in df.items():
        v.replace("", float("NaN"), inplace=True)
        v.dropna(how='all', axis=1, inplace=True)
        # Txt(os.path.join(abs_data_path, "header_{}.txt".format(k))).write_and_close(", ".join(v.columns.values))
        print("Header {}: {}".format(k, ", ".join(v.columns.values)))

    # Build GitHub links from the SonarQube list of projects
    github_links: set[GitHubBean] = set()
    for k, v in df.items():
        for (organization, project), group_df in v.groupby(["organization", 'project']):
            name = project[project.index("_") + 1:] if '_' in project else project
            github_links.add(GitHubBean(flags["clone_path"], organization, name, project))
    github_links: list[GitHubBean] = sorted(github_links, key=lambda x: x.local_path)

    # Remove projects not actually analyzed by SonarQube
    github_links: list[GitHubBean] = list(filter(lambda x: (x.sonar_name in dfa["project"].values), github_links))

    # Clone repositories locally
    bar = MyProgressBar(len(github_links))
    for link in github_links:
        bar.update("Cloning {}".format(link.url))
        clone_project(link)
        break
    bar.close()

    # Instantiate Readability
    readability = Readability(flags["readability_tool"], os.path.join(flags["data_path"], "tmp.java"))
    bar = MyProgressBar(len(github_links))

    # Prepare the CSV for the final analysis
    csvfile = open(flags["output_csv"], 'w', newline='', encoding="utf-8")
    fieldnames = ["github", "commit_hash", "modified_file_count", "file_path"] + readability.measure_list() + dfm.columns.to_list()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',', extrasaction='ignore')
    writer.writeheader()

    # Get metrics per project
    for link in github_links:
        bar.update("Parsing {}".format(link.url))
        process = ProcessMetrics(link.local_path)

        # Get start and stop datatime in accord to SonarQube analyses
        df_sel = dfa[(dfa["organization"] == "apache") & (dfa["project"] == link.sonar_name)]
        # TODO. Change the following
        # start_date = datetime.strptime(min(df_sel["date"]), "%Y-%m-%d %H:%M:%S")
        start_date = datetime.strptime("2021-08-26 11:16:26", "%Y-%m-%d %H:%M:%S")
        stop_date = datetime.strptime(max(df_sel["date"]), "%Y-%m-%d %H:%M:%S")

        # Traverse commits from the oldest to the latest in the selected interval time
        repo = Repository(link.local_path, since=start_date, to=stop_date, only_no_merge=True, only_modifications_with_file_types=[".java"])
        for commit in repo.traverse_commits():
            file_count = len(commit.modified_files)
            if file_count < 100:
                msg = commit.msg.lower()

                # Search for SonarQube (analyses) metrics, if any
                sonar_analyses = dfa[(dfa["project"] == link.sonar_name) & (dfa["revision"] == commit.hash)]
                if not sonar_analyses.empty:

                    # Prepare results
                    csv_dict: Dict[str, str] = {
                        "github": link.url,
                        "commit_hash": commit.hash,
                        "modified_file_count": file_count,
                    }

                    # Append sonar's measures
                    sonar_measures = dfm[dfm["analysis_key"] == sonar_analyses["analysis_key"].iloc[0]].head(1).to_dict("index")
                    csv_dict.update(list(sonar_measures.values())[0])

                    # Traverse files
                    for mod, file_index in zip(commit.modified_files, range(1, file_count + 1)):
                        bar.update("Parsing {} file {}/{}".format(link.url, file_index, file_count))

                        # Get a list (per file) of the last modified lines by using git blame
                        # The following is a computational expensive operation!
                        process_metrics = process.get_process_metrics(commit.hash, get_file_path(mod), commit.author)

                        # TODO. Working on process metrics

                        # Calculate readability
                        readability_delta = readability.get_delta(mod.source_code_before, mod.source_code)

                        # Append readability delta
                        if readability_delta is not None:
                            csv_dict |= readability_delta

                            csv_dict["file_path"] = get_file_path(mod)
                            writer.writerow(csv_dict)
                            csvfile.flush()
                else:
                    print("Cannot find SonarQube analysis for {} {}".format(link.url + "/commit/" + commit.hash, commit.committer_date))
                # break
            # break
        break
    csvfile.close()
    bar.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print("*** Started ***")

    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", help="CSV output filename", type=str, default="forgetting_output.csv")
    parser.add_argument("-r", "--readability", help="Readability input tool", type=str, default="rsm.jar")
    parser.add_argument("-d", "--data_path", help="Input data path", type=str, default="data")
    parser.add_argument("-c", "--clone_path", help="Input path to clone GitHub project", type=str, default="cloned")
    parser.add_argument("-sa", "--sonar_analyses", help="SonarQube analyses file", type=str, default="sonar_analyses_short.csv")
    parser.add_argument("-si", "--sonar_issues", help="SonarQube issues file", type=str, default="sonar_issues_short.csv")
    parser.add_argument("-sm", "--sonar_measures", help="SonarQube measures file", type=str, default="sonar_measures.csv")
    args = parser.parse_args()

    # Check for user's flags

    # Check 'readability' directory
    if args.readability is None or not os.path.exists(args.data_path) or not os.path.isdir(args.data_path):
        print("Invalid --readability argument: {}".format(args.data_path))
        exit(-1)
    abs_readability = os.path.abspath(args.readability)

    # Check 'data' directory
    if args.data_path is None or not os.path.exists(args.data_path) or not os.path.isdir(args.data_path):
        print("Invalid --data_path argument: {}".format(args.data_path))
        exit(-1)
    abs_data_path = os.path.abspath(args.data_path)

    # Output CSV
    abs_output_csv = os.path.abspath(os.path.join(abs_data_path, args.output))

    # Check 'data' directory
    if args.clone_path is None:
        print("Invalid --clone_path argument: {}".format(args.clone_path))
        exit(-1)
    abs_clone_path = os.path.join(abs_data_path, args.clone_path)
    if not os.path.exists(abs_clone_path):
        os.makedirs(abs_clone_path)

    # Build absolute 'analyses' file path and check it exists
    abs_sonar_analyses = os.path.join(abs_data_path, args.sonar_analyses)
    if os.path.exists(abs_sonar_analyses) and not os.path.isfile(abs_sonar_analyses):
        print("Invalid --sonar_analyses argument: {}".format(args.sonar_analyses))
        exit(-1)

    # Build absolute 'analyses' file path and check it exists
    abs_sonar_issues = os.path.join(abs_data_path, args.sonar_issues)
    if os.path.exists(abs_sonar_issues) and not os.path.isfile(abs_sonar_issues):
        print("Invalid --sonar_analyses argument: {}".format(args.sonar_issues))
        exit(-1)

    # Build absolute 'analyses' file path and check it exists
    abs_sonar_measures = os.path.join(abs_data_path, args.sonar_measures)
    if os.path.exists(abs_sonar_measures) and not os.path.isfile(abs_sonar_measures):
        print("Invalid --sonar_analyses argument: {}".format(args.sonar_measures))
        exit(-1)

    option_flags = {
        'readability_tool': abs_readability,
        'data_path': abs_data_path,
        'clone_path': abs_clone_path,
        'output_csv': abs_output_csv,
        'sonar_analyses_path': abs_sonar_analyses,
        'sonar_issues_path': abs_sonar_issues,
        'sonar_measures_path': abs_sonar_measures,
    }

    main(option_flags)

    print("*** Ended ***")
