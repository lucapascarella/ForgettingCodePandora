import sys
import github
import pause
from datetime import datetime
from github import Github, Repository
from typing import List, Dict


class GithubParallelTraversing:
    def __init__(self, tokens: List[str], out_buffer=sys.stdout):
        self.out_buffer = out_buffer
        self.name = None

        self.token_map: dict[str, str] = dict()
        self.gh_api_list: set[Github] = set()
        for token in tokens:
            try:
                # Prepare new GitHub connection, token based authentication
                gh_api = Github(auth=github.Auth.Token(token))
                self.out_buffer.write("GitHub user {} has {} requests per hour, used {}\n"
                                      .format(gh_api.get_user().login, gh_api.get_rate_limit().core.limit, gh_api.get_rate_limit().core.used))
                self.gh_api_list.add(gh_api)
                self.token_map[gh_api.get_user().login] = token
            except github.GithubException as e:
                self.out_buffer.write("Invalid token: {}\n".format(token))

    def close(self):
        for gh_api in self.gh_api_list:
            self.out_buffer.write("GitHub user {} still has {}/{} requests\n"
                                  .format(gh_api.get_user().login, gh_api.get_rate_limit().core.remaining, gh_api.get_rate_limit().core.limit))

    def waiting_for_reset(self, gh: Github) -> float:
        core = gh.get_rate_limit().core
        reset_utc_epoch = core.reset.timestamp()
        current_utc_epoch = datetime.now().timestamp()
        diff_utc_epoch = reset_utc_epoch - current_utc_epoch

        token = self.token_map[gh.get_user().login]
        self.out_buffer.write("GitHub API, {}/{} rate-limit reached! User {} with token '{}' stops for {} seconds, restarts at {}\n".
                              format(core.used, core.limit, gh.get_user().login, token, diff_utc_epoch, datetime.fromtimestamp(reset_utc_epoch)))
        pause.seconds(diff_utc_epoch)
        return diff_utc_epoch

    def get_github_api(self, min_requests: int) -> Github:
        # Sort in descending order, consume many pending requests first
        github_apis: list[Github] = sorted(self.gh_api_list, key=lambda x: x.get_rate_limit().core.remaining, reverse=True)

        # If too few request are remaining, wait for reset time
        while github_apis[0].get_rate_limit().core.remaining < min_requests:
            # Sort in ascending order, smaller waiting time first
            github_apis_waiting: list[Github] = sorted(self.gh_api_list, key=lambda x: x.get_rate_limit().core.reset.timestamp())
            self.waiting_for_reset(github_apis_waiting[0])

            # Sort in descending order, consume many pending requests first
            github_apis: list[Github] = sorted(self.gh_api_list, key=lambda x: x.get_rate_limit().core.remaining, reverse=True)

        # self.out_buffer.write("Using {} that has {}/{}\n".format(github_apis[0].get_user().login, github_apis[0].get_rate_limit().core.remaining,
        #                                                          github_apis[0].get_rate_limit().core.limit))
        return github_apis[0]

    def get_repo_api(self, min_requests: int) -> Repository:
        gh_api = self.get_github_api(min_requests)
        return gh_api.get_repo(self.name)

    def get_repo_details(self, name: str) -> Dict[str, str]:
        self.name = name
        gh_api = self.get_github_api(10)

        repo_api = gh_api.get_repo(self.name)
        return {'name': self.name,
                'language': repo_api.language,
                'created_at': repo_api.created_at,
                'default_branch': repo_api.default_branch,
                'description': repo_api.description,
                'fork_count': repo_api.forks,
                'url': repo_api.html_url}

    def get_pull_list(self, start: datetime, stop: datetime) -> List[int]:
        gh_api = self.get_github_api(10)
        gh_repo = gh_api.get_repo(self.name)

        pull_list: list[int] = []
        for pull in gh_repo.get_pulls(state="all", sort="created"):

            if start <= pull.created_at <= stop:
                pull_list.append(pull.number)

            # Check for API rate limit
            if gh_api.get_rate_limit().core.remaining < 10:
                self.waiting_for_reset(gh_api)

        return pull_list

    def get_pull_details(self, number: int) -> Dict[str, str]:
        gh_api = self.get_github_api(10)
        gh_repo = gh_api.get_repo(self.name)
        pull = gh_repo.get_pull(number)

        return {'pull_number': pull.number, 'html_url': pull.html_url, 'branch': pull.head.ref,
                'title': pull.title, 'body': pull.body, 'state': pull.state, 'merged': pull.merged,
                'comment_count': pull.comments, 'commit_count': pull.commits, 'changed_file_count': pull.changed_files,
                'total_addition_count': pull.additions, 'total_deletion_count': pull.deletions,
                'created_at': pull.created_at, 'merged_at': pull.merged_at, 'closed_at': pull.closed_at, 'updated_at': pull.updated_at,
                'created_by_login': pull.user.login, 'created_by_name': pull.user.name, 'created_by_email': pull.user.email,
                'merge_commit': pull.merge_commit_sha, 'base_commit': pull.base.sha, 'head_commit': pull.head.sha}

    def get_pull_commit_list(self, number: int) -> List[str]:
        gh_api = self.get_github_api(10)
        gh_repo = gh_api.get_repo(self.name)

        commit_list: list[str] = []
        for commit in gh_repo.get_pull(number).get_commits():
            commit_list.append(commit.sha)
            # print("Login {}".format(commit.author.login))

            # Check for API rate limit
            if gh_api.get_rate_limit().core.remaining < 10:
                self.waiting_for_reset(gh_api)

        return commit_list

    def get_pull_issue_list(self, number: int, start: datetime, stop: datetime) -> List[int]:
        gh_api = self.get_github_api(10)
        gh_repo = gh_api.get_repo(self.name)

        issue_list: list[int] = []
        for issue in gh_repo.get_pull(number).get_issue_comments():
            if start <= issue.created_at <= stop:
                issue_list.append(issue.id)
            # Check for API rate limit
            if gh_api.get_rate_limit().core.remaining < 10:
                self.waiting_for_reset(gh_api)
        return issue_list

    def get_pull_issue_details(self, pl_number: int, issue_number: int) -> Dict[str, str]:
        gh_api = self.get_github_api(10)
        gh_repo = gh_api.get_repo(self.name)
        pull = gh_repo.get_pull(pl_number)
        issue = pull.get_issue_comment(issue_number)

        return {'pull_issue_number': issue.id, 'html_url': issue.html_url,
                'created_at': issue.created_at, 'updated_at': issue.updated_at,
                'user_login': issue.user.login, 'user_name': issue.user.name, 'user_email': issue.user.email}

    def get_issue_list(self, start: datetime, stop: datetime) -> List[int]:
        gh_api = self.get_github_api(10)
        gh_repo = gh_api.get_repo(self.name)

        issue_list: list[int] = []
        for issue in gh_repo.get_issues(state="all", sort="created"):
            if start <= issue.created_at <= stop:
                issue_list.append(issue.number)

            # Check for API rate limit
            if gh_api.get_rate_limit().core.remaining < 10:
                self.waiting_for_reset(gh_api)

        return issue_list

    def get_issue_details(self, number: int) -> Dict[str, str]:
        gh_api = self.get_github_api(10)
        gh_repo = gh_api.get_repo(self.name)
        issue = gh_repo.get_issue(number)

        return {'issue_number': issue.number, 'html_url': issue.html_url,
                'title': issue.title, 'body': issue.body, 'state': issue.state,
                'comment_count': issue.comments,
                'created_at': issue.created_at, 'closed_at': issue.closed_at, 'updated_at': issue.updated_at,
                'created_by_login': issue.user.login, 'created_by_name': issue.user.name, 'created_by_email': issue.user.email}
