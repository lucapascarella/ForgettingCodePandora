import subprocess
import re
from typing import Tuple, List


class Readability:
    def __init__(self, readability_tool: str):
        self.readability_tool = readability_tool
        self.timeout = 60 * 60 * 1  # 1 hour

    def run_command(self, command: List[str]) -> Tuple[str, str]:
        shell_result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=self.timeout)
        try:
            stdout = shell_result.stdout.decode('utf-8')
            stderr = shell_result.stderr.decode('utf-8')
            return stdout, stderr
        except UnicodeDecodeError as exception:
            # srcml_bean.exception = exception
            print("UnicodeDecodeError: 'utf-8' codec can't decode byte")
        except OSError as exception:
            # srcml_bean.exception = exception
            print("BlockingIOError: [Errno 11] Resource temporarily unavailable")
        except subprocess.TimeoutExpired as exception:
            # srcml_bean.exception = exception
            print("SrcML timeout")
        raise Exception

    def run_readability_simple(self, filename: str) -> float:
        command = ['java', '-jar', self.readability_tool, filename]
        out, err = self.run_command(command)

        if out is not None and err is not None:
            if "File not found:" in err:
                print(err)
                raise Exception
            elif out != '':
                regex = r"(?i)\.java\s+(\d+\.\d+)"
                reg_res = re.search(regex, out)

                if reg_res and len(reg_res.groups()) == 1:
                    readability = float(reg_res.group(1))
                    return readability
        return 0

    def run_readability_extended(self, filename: str) -> float:
        command = ['java', '-cp', self.readability_tool, 'it.unimol.readability.metric.runnable.ExtractMetrics', filename]
        out, err = self.run_command(command)

        if out is not None and err is not None:
            if "File not found:" in err:
                print(err)
                raise Exception
            elif out != '':
                # Commented words - CIC
                cic_avg = cic_max = None
                reg_res = re.search(r"Commented words AVG:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    cic_avg = float(reg_res.group(1))
                reg_res = re.search(r"Commented words MAX:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    cic_max = float(reg_res.group(1))

                # Synonym commented words - CIC syn
                cic_syn_avg = cic_syn_max = None
                reg_res = re.search(r"Synonym commented words AVG:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    cic_syn_avg = float(reg_res.group(1))
                reg_res = re.search(r"Synonym commented words MAX:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    cic_syn_max = float(reg_res.group(1))

                # Identifiers words - ITID
                itid_avg = itid_min = None
                reg_res = re.search(r"Identifiers words AVG:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    itid_avg = float(reg_res.group(1))
                reg_res = re.search(r"Identifiers words MIN:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    itid_min = float(reg_res.group(1))

                # Abstractness words - NMI
                nmi_avg = nmi_min = nmi_max = None
                reg_res = re.search(r"Abstractness words\s+AVG:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    nmi_avg = float(reg_res.group(1))
                reg_res = re.search(r"Abstractness words MIN:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    nmi_min = float(reg_res.group(1))
                reg_res = re.search(r"Abstractness words MAX:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    nmi_max = float(reg_res.group(1))

                # # Comments readability - CR
                cr = None
                reg_res = re.search(r"Comments readability:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    cr = float(reg_res.group(1))

                # Number of senses - NM
                nm_avg = nm_max = None
                reg_res = re.search(r"Number of senses AVG:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    nm_avg = float(reg_res.group(1))
                reg_res = re.search(r"Number of senses MAX:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    nm_max = float(reg_res.group(1))

                # Text Coherence - TC
                tc_avg = tc_min = tc_max = None
                reg_res = re.search(r"Text Coherence AVG:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    tc_avg = float(reg_res.group(1))
                reg_res = re.search(r"Text Coherence MIN:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    tc_min = float(reg_res.group(1))
                reg_res = re.search(r"Text Coherence MAX:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    tc_max = float(reg_res.group(1))

                # Semantic Text Coherence - NOC
                noc_std = noc_nor = None
                reg_res = re.search(r"Semantic Text Coherence Standard:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    noc_std = float(reg_res.group(1))
                reg_res = re.search(r"Semantic Text Coherence Normalized:\s+(\d+\.\d+|NaN)", out)
                if reg_res and len(reg_res.groups()) == 1:
                    noc_nor = float(reg_res.group(1))

        return 0
