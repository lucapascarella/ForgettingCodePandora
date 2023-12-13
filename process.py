import re
from typing import List, Tuple, Optional, Dict

from git import GitCommandError
from pydriller import Git
from pydriller.domain.developer import Developer


class ProcessMetrics:
    def __init__(self, local_repo_path: str):
        self.py_git = Git(local_repo_path)
        self.authored_lines = {}

    def get_process_metrics(self, commit_hash: str, file_path: str, author: Developer) -> Tuple[Optional[float], Optional[float]]:
        try:
            py_blame = self.py_git._get_blame(commit_hash, file_path)

            if py_blame is not None:
                distinct_authors: Dict[str, int] = {}
                # blamed_lines = []
                for line in py_blame:
                    reg_res = re.search(r"^([1-9a-fA-F]{8})(\s.*)\((.*)\s(\d\d\d\d-\d\d-\d\d)\s(\d\d:\d\d:\d\d)\s([-+]\d\d\d\d)\s+(\d+)\)\s(.*)$", line.strip())
                    if reg_res and len(reg_res.groups()) == 8:
                        # blamed_hash = reg_res.group(1).replace('^', '').strip()
                        # blamed_file = reg_res.group(2).strip()
                        blamed_author = reg_res.group(3).strip()
                        # blamed_date = reg_res.group(4).strip()
                        # blamed_time = reg_res.group(5).strip()
                        # blamed_zone = reg_res.group(6).strip()
                        # blamed_line = reg_res.group(7).strip()
                        # blamed_src = reg_res.group(8).strip()
                        # blamed_lines.append((blamed_hash, blamed_file, blamed_author, blamed_date, blamed_time, blamed_zone, blamed_line, blamed_src))

                        # OEXP Measure
                        if blamed_author not in distinct_authors:
                            distinct_authors[blamed_author] = 0
                        distinct_authors[blamed_author] += 1

                    distinct_author_count = len(distinct_authors)
                    authored_line_count = distinct_authors.get(author.name, 0)
                    src_lines = len(py_blame)

                    # blamed_lines.append((None, None, None, None, None, None, None, None))

                # self.calculate_lmod(blamed_lines, author.name)
        except GitCommandError as exception:
            print(exception)
        return None, None

    # def calculate_lmod(self, blamed_lines: List[Tuple[str, str, str, str, str, str, str, str]], author: str) -> float:
    #     return 0
