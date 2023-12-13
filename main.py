import argparse
import os
import github
import pandas as pd
from datetime import datetime
from typing import Dict
from git import NoSuchPathError
from pydriller import Repository

import utils
from utils import GitHubBean, MyProgressBar
from githubAPI import GithubParallelTraversing
from readability import Readability


def main(flags: Dict[str, str]) -> None:
    # Column names: organization, project, analysis_key, date, project_version ,revision, processed, ingested_at
    dfa = pd.read_csv(flags["sonar_analyses_path"], sep=',')
    orig_len = len(dfa.index)
    dfa = dfa.drop_duplicates()
    print("Removed {} duplicated lines of {} from {}".format(orig_len - len(dfa.index), orig_len, flags["sonar_analyses_path"]))

    # Column names: organization, project, current_analysis_key, creation_analysis_key, issue_key, type, rule, severity, status, resolution, effort, debt,
    # tags, creation_date, update_date, close_date, processed, ingested_at
    dfi = pd.read_csv(flags["sonar_issues_path"], sep=',')
    orig_len = len(dfi.index)
    dfi = dfi.drop_duplicates()
    print("Removed {} duplicated lines of {} from {}".format(orig_len - len(dfi.index), orig_len, flags["sonar_issues_path"]))

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
    orig_len = len(dfm.index)
    dfm = dfm.drop_duplicates()
    print("Removed {} duplicated lines of {} from {}".format(orig_len - len(dfm.index), orig_len, flags["sonar_measures_path"]))

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
    github_beans: set[GitHubBean] = set()
    for k, v in df.items():
        for (organization, project), group_df in v.groupby(["organization", 'project']):
            name = project[project.index("_") + 1:] if '_' in project else project
            github_beans.add(GitHubBean(flags["clone_path"], organization, name, project))
    github_beans: list[GitHubBean] = sorted(github_beans, key=lambda x: x.local_path)

    # Remove projects not actually analyzed by SonarQube
    github_beans: list[GitHubBean] = list(filter(lambda x: (x.sonar_name in dfa["project"].values), github_beans))

    # Clone repositories locally
    if flags["always_clone_first"]:
        bar = MyProgressBar(len(github_beans))
        for index, gh_bean in enumerate(github_beans):
            bar.update("Cloning {}".format(gh_bean.url))
            utils.clone_project(gh_bean)
        bar.close()

    # Instantiate Readability
    readability = Readability(flags["readability_tool"], flags["temp_filename"], int(flags['readability_timeout']))

    # GitHub API parser
    ght = GithubParallelTraversing(flags["tokens"].split(','))

    # Get Sonar metrics per project
    project_index = 0
    for gh_bean in github_beans:
        try:
            # Get datatime interval in accord to SonarQube analyses
            df_sel = dfa[(dfa["organization"] == "apache") & (dfa["project"] == gh_bean.sonar_name)]
            start_date = datetime.strptime(min(df_sel["date"]), "%Y-%m-%d %H:%M:%S")
            stop_date = datetime.strptime(max(df_sel["date"]), "%Y-%m-%d %H:%M:%S")

            # Traverse commits from the oldest to the latest in the selected interval time
            repo = Repository(gh_bean.local_path, since=start_date, to=stop_date, only_no_merge=True, only_modifications_with_file_types=[".java"])
            project_status = "{}/{})".format(project_index, len(github_beans))
            print("{} Analyzing {} from {} to {}".format(project_status, gh_bean.url, start_date, stop_date))
            line_count = 0
            discarded_commit_count = commit_count = 0

            # Count OEXP metric
            lines_per_author: dict[str, int] = {}
            for commit in repo.traverse_commits():
                commit_count += 1
                if commit.author.email not in lines_per_author:
                    lines_per_author["OEXP_" + commit.author.email] = 0
                lines_per_author["OEXP_" + commit.author.email] += commit.lines

            sonar_commits = len(df_sel.groupby(["analysis_key"])["analysis_key"])
            gh_bean.print_report("In {}, from {} to {}, pydriller found {} commits, SonarQube has {} commits analyzed. Missing {} commits"
                                 .format(gh_bean.url, start_date, stop_date, commit_count, sonar_commits, commit_count - sonar_commits))

            # Prepare the CSV for the final analysis
            fieldnames = (["github", "commit_hash", "committer_date", "modified_file_count", "file_path", "LMOD"]
                          + readability.measure_list() + dfm.columns.to_list() + dfi.columns.to_list() + sorted(lines_per_author, reverse=True))
            gh_bean.create_csvs(fieldnames)

            # Reset OEXP
            lines_per_author = lines_per_author.fromkeys(lines_per_author, 0)

            # Get all pull requests and issues
            repo_details = ght.get_repo_details(gh_bean.owner + "/" + gh_bean.name)

            # Traverse Pull Requests
            pull_list = ght.get_pull_list()
            gh_bean.create_progress_bar(len(pull_list))
            for pl_number in pull_list:
                gh_bean.update_bar("{} Getting pull {}".format(project_status, pl_number))
                pull_details = ght.get_pull_details(pl_number)
                commit_list = ght.get_pull_commit_list(pl_number)
                gh_bean.append_pull({"commit_list": commit_list} | repo_details | pull_details)

            # Traverse Issues
            issue_list = ght.get_issue_list()
            gh_bean.create_progress_bar(len(issue_list))
            for issue_number in issue_list:
                gh_bean.update_bar("{} Getting issue {}".format(project_status, issue_number))
                issue_details = ght.get_issue_details(issue_number)
                gh_bean.append_issue(repo_details | issue_details)

            # We already know the number of commits to traverse, so we can create the progress bar
            gh_bean.create_progress_bar(commit_count)
            for commit in repo.traverse_commits():
                gh_bean.update_bar("{} Analyzing {}".format(project_status, gh_bean.url))

                # Count number of globally authored lines
                line_count += commit.lines
                # Count number of authored lines per author
                lines_per_author["OEXP_" + commit.author.email] += commit.lines

                # Search for SonarQube (analyses) metrics, if any
                sonar_analyses = dfa[(dfa["project"] == gh_bean.sonar_name) & (dfa["revision"] == commit.hash)]
                gh_bean.print_report("Found {} sonar analyses for {} {}".format(len(sonar_analyses["analysis_key"]), commit.hash, commit.committer_date))
                sonar_analysis_key = None if sonar_analyses.empty else sonar_analyses["analysis_key"].iloc[0]

                modified_files = []
                for mod_file in commit.modified_files:
                    file_path = mod_file.new_path if mod_file.new_path else mod_file.old_path
                    modified_files.append(file_path)

                # Generate statistics
                file_count = len(modified_files)
                stat_dict: dict[str, str] = {
                    "project": gh_bean.url,
                    "commit_hash": commit.hash,
                    "committer_date": commit.committer_date,
                    "modified_files": modified_files,
                    "modified_file_count": file_count,
                    "author_email": commit.author.email,
                    "committer_email": commit.committer.email,
                    "sonar_analyses": len(sonar_analyses["analysis_key"]),
                    "sonar_measures": 0,
                    "sonar_issues": 0,
                }

                if not sonar_analyses.empty:
                    if file_count < 500:
                        # Prepare results
                        # msg = commit.msg.lower()
                        result_dict: dict[str, str] = {
                            "github": gh_bean.url,
                            "commit_hash": commit.hash,
                            "committer_date": commit.committer_date,
                            "modified_file_count": file_count,
                        }

                        # Append sonar's measures.
                        # sonar_measures.csv may have multiple measures corresponding to the same analysis_key or even zero
                        sub_dfm = dfm[dfm["analysis_key"] == sonar_analysis_key]
                        if not sub_dfm.empty:
                            result_dict.update(sub_dfm.iloc[[0]].to_dict('records')[0])
                            stat_dict["sonar_measures"] = str(len(sub_dfm["analysis_key"]))
                        else:
                            gh_bean.print_report("Found 0 measures for {}".format(sonar_analysis_key))

                        # Append sonar's issues
                        sub_dfi = dfi[dfi["current_analysis_key"] == sonar_analysis_key]
                        if not sub_dfi.empty:
                            result_dict.update(sub_dfi.iloc[[0]].to_dict('records')[0])
                            stat_dict["sonar_issues"] = str(len(sub_dfm["analysis_key"]))
                        else:
                            gh_bean.print_report("Found 0 issues for {}".format(sonar_analysis_key))

                        # OEXP. % of lines authored in the project up to considered commit
                        result_dict.update({k: v / line_count * 100 for k, v in lines_per_author.items()})

                        # LMOD
                        lines_in_commit = 0
                        for mod in commit.modified_files:
                            if mod.source_code is not None:
                                lines_in_commit += mod.source_code.count("\n")
                        result_dict["LMOD"] = str(commit.lines / lines_in_commit * 100) if lines_in_commit != 0 else 0

                        # Traverse repo's files
                        readability_delta_list: list[dict[str, float]] = []
                        for mod, file_index in zip(commit.modified_files, range(1, file_count + 1)):
                            if mod.filename.endswith(".java"):
                                gh_bean.update_bar(
                                    "{} Parsing {}/commit/{} file {}/{}".format(project_status, gh_bean.url, commit.hash, file_index, file_count))

                                # Get a list (per file) of the last modified lines by using git blame
                                # The following is a computational expensive operation!
                                # process_metrics = process.get_process_metrics(commit.hash, get_file_path(mod), commit.author)

                                # Calculate readability
                                readability_delta = readability.get_delta(mod.source_code_before, mod.source_code)

                                # Append readability delta
                                if readability_delta is not None:
                                    if flags["analysis_per_file"]:
                                        result_dict.update(readability.expand_dictionary(readability_delta))
                                        result_dict["file_path"] = utils.get_file_path(mod)
                                        gh_bean.append_result(result_dict)
                                    else:
                                        readability_delta_list.append(readability.expand_dictionary(readability_delta))
                                else:
                                    gh_bean.print_report("Readability missing for {}/commit/{}".format(gh_bean.url, commit.hash))

                        # Aggregate readability by commit
                        if not flags["analysis_per_file"]:
                            # Get average of delta measures
                            delta_avg = {}
                            for key in readability.measure_list():
                                delta_avg[key] = 0
                                for delta in readability_delta_list:
                                    if delta[key] is not None:
                                        delta_avg[key] += delta[key]
                                delta_avg[key] / len(readability.measure_list())

                            result_dict.update(delta_avg)
                            gh_bean.append_result(result_dict)

                    else:
                        gh_bean.print_exception("{}/commit/{} has too many files to run readability tool".format(gh_bean.url, commit.hash))
                else:
                    discarded_commit_count += 1
                    gh_bean.print_exception(
                        "{}. Cannot find {} {} in {}".format(discarded_commit_count, commit.hash, commit.committer_date, flags["sonar_analyses_path"]))

                # Append stat
                gh_bean.append_stat(stat_dict)

            gh_bean.print_exception("{} {}/{} missing commit in SonarQube for {}".format(project_status, discarded_commit_count, commit_count, gh_bean.url))
            gh_bean.close()
        except NoSuchPathError as exception:
            print("Skipping {} due to {}".format(gh_bean.url, exception))

        # Increment project index, zip does not work in PyCharm with code assistant
        project_index += 1


if __name__ == '__main__':
    print("*** Started ***")

    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--readability", help="Readability input tool", type=str, default="rsm.jar")
    parser.add_argument("-d", "--data_path", help="Input data path", type=str, default="data")
    parser.add_argument("-c", "--clone_path", help="Input path to clone GitHub project", type=str, default="cloned")
    parser.add_argument("-sa", "--sonar_analyses", help="SonarQube analyses file", type=str, default="sonar_analyses.csv")
    parser.add_argument("-si", "--sonar_issues", help="SonarQube issues file", type=str, default="sonar_issues.csv")
    parser.add_argument("-sm", "--sonar_measures", help="SonarQube measures file", type=str, default="sonar_measures.csv")
    parser.add_argument("-o", "--readability_timeout", help="Readability timout in seconds", type=int, default=300)
    parser.add_argument("-t", "--temp", help="Absolute temporary path. E.g., RAMDisk mount -t tmpfs -o size=500m tmpfs /mount", type=str, default="temp.java")
    parser.add_argument("-f", "--file_level", help="Save results at file level granularity", type=bool, default=False)
    parser.add_argument('-gt', '--tokens', nargs='*', help='GitHub tokens', required=True)
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

    temp_filename = args.temp_filename
    file_level = args.file_level
    readability_timeout = args.readability_timeout
    # Clean up token list
    tokens = ",".join(args.tokens)

    option_flags = {
        'readability_tool': abs_readability,
        'data_path': abs_data_path,
        'clone_path': abs_clone_path,
        'sonar_analyses_path': abs_sonar_analyses,
        'sonar_issues_path': abs_sonar_issues,
        'sonar_measures_path': abs_sonar_measures,
        'temp_filename': temp_filename,
        'analysis_per_file': file_level,
        'readability_timeout': readability_timeout,
        'tokens': tokens,
        'always_clone_first': True,
    }

    main(option_flags)

    print("*** Ended ***")
