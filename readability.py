import subprocess
import re
from enum import Enum
from typing import Tuple, List, Dict, Optional


class MetricType(Enum):
    MIN = 0
    AVG = 1
    MAX = 2

    STD = 0
    NOR = 1


class Readability:
    def __init__(self, readability_tool: str, temp_filename: str, seconds_timeout: int):
        self.exception = None
        self.readability_tool = readability_tool
        self.temp_filename = temp_filename
        self.timeout = seconds_timeout  # 60 * 60 * 1  # 1 hour

    def get_delta(self, source_before: str, source_current: str) -> Optional[Dict[str, Tuple[float, float, float]]]:
        if source_before is not None and source_before:
            if source_current is not None and source_current:
                # Get readability before
                file = open(self.temp_filename, 'w')
                file.write(source_before)
                file.close()
                readability_before = self.run_readability_extended(self.temp_filename)

                # Get readability current
                file = open(self.temp_filename, 'w')
                file.write(source_current)
                file.close()
                readability_current = self.run_readability_extended(self.temp_filename)

                if readability_before is not None and readability_current is not None:
                    return self.calculate_diff(readability_before, readability_current)

        return None

    def expand_dictionary(self, metrics: Dict[str, Tuple[float, float, float]]) -> Dict[str, float]:
        # return {"CIC_AVG": str(metrics["CIC"][MetricType.AVG.value]),
        #         "CIC_MAX": str(metrics["CIC"][MetricType.MAX.value]),
        #
        #         "CIC_syn_AVG": str(metrics["CIC"][MetricType.AVG.value]),
        #         "CIC_syn_MAX": str(metrics["CIC"][MetricType.MAX.value]),
        #
        #         "ITID_MIN": str(metrics["CIC"][MetricType.MIN.value]),
        #         "ITID_AVG": str(metrics["CIC"][MetricType.AVG.value]),
        #
        #         "NMI_MIN": str(metrics["CIC"][MetricType.MIN.value]),
        #         "NMI_AVG": str(metrics["CIC"][MetricType.AVG.value]),
        #         "NMI_MAX": str(metrics["CIC"][MetricType.MAX.value]),
        #
        #         "CR": str(metrics["CIC"][0]),
        #
        #         "NM_AVG": str(metrics["CIC"][MetricType.AVG.value]),
        #         "NM_MAX": str(metrics["CIC"][MetricType.MAX.value]),
        #
        #         "TC_MIN": str(metrics["CIC"][MetricType.MIN.value]),
        #         "TC_AVG": str(metrics["CIC"][MetricType.AVG.value]),
        #         "TC_MAX": str(metrics["CIC"][MetricType.MAX.value]),
        #
        #         "NOC_STD": str(metrics["CIC"][MetricType.STD.value]),
        #         "NOC_NOR": str(metrics["CIC"][MetricType.NOR.value])}
        return {"CIC_AVG": metrics["CIC"][MetricType.AVG.value],
                "CIC_MAX": metrics["CIC"][MetricType.MAX.value],

                "CIC_syn_AVG": metrics["CIC"][MetricType.AVG.value],
                "CIC_syn_MAX": metrics["CIC"][MetricType.MAX.value],

                "ITID_MIN": metrics["CIC"][MetricType.MIN.value],
                "ITID_AVG": metrics["CIC"][MetricType.AVG.value],

                "NMI_MIN": metrics["CIC"][MetricType.MIN.value],
                "NMI_AVG": metrics["CIC"][MetricType.AVG.value],
                "NMI_MAX": metrics["CIC"][MetricType.MAX.value],

                "CR": metrics["CIC"][0],

                "NM_AVG": metrics["CIC"][MetricType.AVG.value],
                "NM_MAX": metrics["CIC"][MetricType.MAX.value],

                "TC_MIN": metrics["CIC"][MetricType.MIN.value],
                "TC_AVG": metrics["CIC"][MetricType.AVG.value],
                "TC_MAX": metrics["CIC"][MetricType.MAX.value],

                "NOC_STD": metrics["CIC"][MetricType.STD.value],
                "NOC_NOR": metrics["CIC"][MetricType.NOR.value]}

    def run_command(self, command: List[str]) -> Tuple[Optional[str], Optional[str]]:
        try:
            shell_result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=self.timeout)
            stdout = shell_result.stdout.decode('utf-8')
            stderr = shell_result.stderr.decode('utf-8')
            return stdout, stderr
        except UnicodeDecodeError as exception:
            self.exception = exception
            print("UnicodeDecodeError: 'utf-8' codec can't decode byte")
        except OSError as exception:
            self.exception = exception
            print("BlockingIOError: [Errno 11] Resource temporarily unavailable")
        except subprocess.TimeoutExpired as exception:
            self.exception = exception
            # print("Caught timeout exception: Readability tool timeout >{} minutes".format(timeout/60))
        return None, None

    def run_readability_simple(self, filename: str) -> float:
        command = ['java', '-jar', self.readability_tool, filename]
        out, err = self.run_command(command)

        if out is not None and err is not None:
            if "File not found:" in err:
                print(err)
                # raise Exception
            elif out != '':
                regex = r"(?i)\.java\s+(\d+\.\d+)"
                reg_res = re.search(regex, out)

                if reg_res and len(reg_res.groups()) == 1:
                    readability = float(reg_res.group(1))
                    return readability
        return 0

    def run_readability_extended(self, filename: str) -> Optional[Dict[str, Tuple[float, float, float]]]:
        command = ['java', '-cp', self.readability_tool, 'it.unimol.readability.metric.runnable.ExtractMetrics', filename]
        out, err = self.run_command(command)

        if out is not None and err is not None:
            if "File not found:" in err:
                print(err)
            elif out != '':
                # Commented words - CIC
                cic_min = self.match_single_group_to_float(r"Commented words MIN:\s+(\d+\.\d+|NaN)", out)
                cic_avg = self.match_single_group_to_float(r"Commented words AVG:\s+(\d+\.\d+|NaN)", out)
                cic_max = self.match_single_group_to_float(r"Commented words MAX:\s+(\d+\.\d+|NaN)", out)

                # Synonym commented words - CIC syn
                cic_syn_min = self.match_single_group_to_float(r"Synonym commented words MIN:\s+(\d+\.\d+|NaN)", out)
                cic_syn_avg = self.match_single_group_to_float(r"Synonym commented words AVG:\s+(\d+\.\d+|NaN)", out)
                cic_syn_max = self.match_single_group_to_float(r"Synonym commented words MAX:\s+(\d+\.\d+|NaN)", out)

                # Identifiers words - ITID
                itid_min = self.match_single_group_to_float(r"Identifiers words MIN:\s+(\d+\.\d+|NaN)", out)
                itid_avg = self.match_single_group_to_float(r"Identifiers words AVG:\s+(\d+\.\d+|NaN)", out)
                itid_max = self.match_single_group_to_float(r"Identifiers words MAX:\s+(\d+\.\d+|NaN)", out)

                # Abstractness words - NMI
                nmi_min = self.match_single_group_to_float(r"Abstractness words MIN:\s+(\d+\.\d+|NaN)", out)
                nmi_avg = self.match_single_group_to_float(r"Abstractness words AVG:\s+(\d+\.\d+|NaN)", out)
                nmi_max = self.match_single_group_to_float(r"Abstractness words MAX:\s+(\d+\.\d+|NaN)", out)

                # Comments readability - CR
                cr = self.match_single_group_to_float(r"Comments readability:\s+(\d+\.\d+|NaN)", out)

                # Number of senses - NM
                nm_min = self.match_single_group_to_float(r"Number of senses MIN:\s+(\d+\.\d+|NaN)", out)
                nm_avg = self.match_single_group_to_float(r"Number of senses AVG:\s+(\d+\.\d+|NaN)", out)
                nm_max = self.match_single_group_to_float(r"Number of senses MAX:\s+(\d+\.\d+|NaN)", out)

                # Text Coherence - TC
                tc_min = self.match_single_group_to_float(r"Text Coherence MIN:\s+(\d+\.\d+|NaN)", out)
                tc_avg = self.match_single_group_to_float(r"Text Coherence AVG:\s+(\d+\.\d+|NaN)", out)
                tc_max = self.match_single_group_to_float(r"Text Coherence MAX:\s+(\d+\.\d+|NaN)", out)

                # Semantic Text Coherence - NOC
                noc_std = self.match_single_group_to_float(r"Semantic Text Coherence Standard:\s+(\d+\.\d+|NaN)", out)
                noc_nor = self.match_single_group_to_float(r"Semantic Text Coherence Normalized:\s+(\d+\.\d+|NaN)", out)

                return {
                    "CIC": (cic_min, cic_avg, cic_max),
                    "CIC_syn": (cic_syn_min, cic_syn_avg, cic_syn_max),
                    "ITID": (itid_min, itid_avg, itid_max),
                    "NMI": (nmi_min, nmi_avg, nmi_max),
                    "CR": (cr,),  # Last comma force a tuple
                    "NM": (nm_min, nm_avg, nm_max),
                    "TC": (tc_min, tc_avg, tc_max),
                    "NOC": (noc_std, noc_nor),
                }
        return None

    @staticmethod
    def match_single_group_to_float(pattern: str, string: str) -> Optional[float]:
        reg_res = re.search(pattern, string)
        if reg_res and len(reg_res.groups()) == 1:
            return float(reg_res.group(1))
        return None

    @staticmethod
    def calculate_diff(r1: Dict[str, Tuple[float, float, float]], r2: Dict[str, Tuple[float, float, float]]) -> Dict[str, Tuple[float, float, float]]:
        rtn = {}
        for (k1, v1), (k2, v2) in zip(r1.items(), r2.items()):
            rtn[k1] = tuple(map(lambda t1, t2: None if t1 is None or t2 is None else t1 - t2, v1, v2))
        return rtn

    @staticmethod
    def measure_list() -> List[str]:
        return ["CIC_AVG", "CIC_MAX",
                "CIC_syn_AVG", "CIC_syn_MAX",
                "ITID_MIN", "ITID_AVG",
                "NMI_MIN", "NMI_AVG", "NMI_MAX",
                "CR",
                "NM_AVG", "NM_MAX",
                "TC_MIN", "TC_AVG", "TC_MAX",
                "NOC_STD", "NOC_NOR"]
