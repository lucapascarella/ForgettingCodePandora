import os
import csv
from csv import DictWriter

from tqdm import tqdm
from threading import Lock
from typing import Dict, List, Optional, TextIO
from pydriller import ModifiedFile, ModificationType, Repository, Git
from git import GitCommandError


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

        self.file_report = None
        self.file_exception = None
        self.file_result = None
        self.file_stat = None
        self.file_pull = None
        self.file_issue = None
        self.result_writer = None
        self.stat_writer = None
        self.pull_writer = None
        self.issue_writer = None
        self.bar = None

    def print_report(self, message: str) -> None:
        if self.file_report is None:
            self.file_report = open(os.path.join(self.clone_path, "{}_report.txt".format(self.name)), 'w')
        self.file_report.write(message)
        if not message.endswith('\n') and not message.endswith('\r'):
            self.file_report.write("\r\n")
        self.file_report.flush()

    def print_exception(self, message: str) -> None:
        if self.file_exception is None:
            self.file_exception = open(os.path.join(self.clone_path, "{}_exception.txt".format(self.name)), 'w')
        self.file_exception.write(message)
        if not message.endswith('\n') and not message.endswith('\r'):
            self.file_exception.write("\r\n")
        self.file_exception.flush()

    # def _create_csv(self, filename: str, header: List[str]) -> tuple[TextIO, DictWriter[str]]:
    def _create_csv(self, filename: str, header: List[str]):
        filename = os.path.join(self.clone_path, "{}_{}.csv".format(self.name, filename))
        file = open(filename, 'w', newline='', encoding="utf-8")
        writer = csv.DictWriter(file, fieldnames=header, delimiter=',', extrasaction='ignore')
        writer.writeheader()
        return file, writer

    def create_csvs(self, header: List[str]) -> None:
        # Result CSV
        self.file_result, self.result_writer = self._create_csv("result", header)

        # Stats CSV
        header = ["project", "commit_hash", "committer_date", "modified_files", "modified_file_count", "author_email", "committer_email", "sonar_analyses",
                  "sonar_measures", "sonar_issues"]
        self.file_stat, self.stat_writer = self._create_csv("stat", header)

        # Pull Requests CSV
        header = ['name', 'language', 'created_at', 'default_branch', 'description', 'fork_count', 'url',
                  'pull_number', 'html_url', 'branch',
                  'title', 'state', 'merged',  # 'body', DO NOT INCLUDE BODY IN CSV
                  'comment_count', 'commit_count', 'changed_file_count',
                  'total_addition_count', 'total_deletion_count',
                  'created_at', 'merged_at', 'closed_at', 'updated_at',
                  'created_by_login', 'created_by_name', 'created_by_email',
                  'merge_commit', 'base_commit', 'head_commit', 'commit_list']
        self.file_pull, self.pull_writer = self._create_csv("pull", header)

        # Issue CSV
        header = ['name', 'language', 'created_at', 'default_branch', 'description', 'fork_count', 'url',
                  'issue_number', 'html_url',
                  'title', 'state', 'comment_count',  # 'body', DO NOT INCLUDE BODY IN CSV
                  'created_at', 'closed_at', 'updated_at',
                  'created_by_login', 'created_by_name', 'created_by_email']
        self.file_issue, self.issue_writer = self._create_csv("issue", header)

    def append_result(self, csv_dict: Dict[str, str]) -> None:
        self.result_writer.writerow(csv_dict)
        self.file_result.flush()

    def append_stat(self, csv_dict: Dict[str, str]) -> None:
        self.stat_writer.writerow(csv_dict)
        self.file_stat.flush()

    def append_pull(self, csv_dict: Dict[str, str]) -> None:
        self.pull_writer.writerow(csv_dict)
        self.file_pull.flush()

    def append_issue(self, csv_dict: Dict[str, str]) -> None:
        self.issue_writer.writerow(csv_dict)
        self.file_issue.flush()

    def close(self):
        self.file_report.close()
        self.file_result.close()
        self.file_stat.close()
        self.file_pull.close()
        self.file_issue.close()
        self.bar.close()

    def create_progress_bar(self, bar_size: int) -> None:
        self.bar = MyProgressBar(bar_size)

    def update_bar(self, message: str) -> None:
        if self.bar is not None:
            self.bar.update(message)

    def get_progress_bar(self) -> MyProgressBar:
        return self.bar

    def __eq__(self, other):
        return self.owner == other.owner and self.name == other.name

    def __hash__(self):
        return hash(('owner', self.owner, 'name', self.name))


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
