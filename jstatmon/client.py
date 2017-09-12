'''Client is responsible for actively monitoring API endpoints.

This module is used to collect a list of REST API endpoints and iterate over
them to log the response content (if it exists), status code, and round trip
time of the request.
'''

from jstatmon.log import setup_logger
from os import access, X_OK, pathsep, environ, setuid, setgid
from os.path import isfile, split, join
from subprocess import Popen, PIPE
from pwd import getpwnam
from logging import DEBUG, INFO
import shlex


class JStatmonClient(object):
    '''jstatmonClient is used to monitor API endpoints.

    Attributes:
        endpoints (:obj:`list` of :obj:`str`): A list of API URIs to monitor
        logger (:obj:`logging.Logger`): A logging engine
    '''

    def __init__(self, verbose=False, environment='prod'):
        '''Initialize a new jstatmon client.

        Args:
            endpoints (:obj:`list` of :obj:`str`): A list of API URIs to monitor
        '''
        if verbose:
            self.logger = setup_logger(DEBUG)
        else:
            self.logger = setup_logger(INFO)
        self.environment = environment

        # metrics from https://docs.oracle.com/javase/8/docs/technotes/tools/unix/jstat.html
        # Replace jstat colums titles with more explicit ones
        # Stats coming from -gc
        self.metric_maps_gc = {
            "S0C": "current_survivor_space_0_capacity_kB",
            "S1C": "current_survivor_space_1_capacity_kB",
            "S0U": "survivor_space_0_utilization_kB",
            "S1U": "survivor_space_1_utilization_kB",
            "EC": "current_eden_space_capacity_kB",
            "EU": "eden_space_utilization_kB",
            "OC": "current_old_space_capacity_kB",
            "OU": "old_space_utilization_kB",
            "MC": "metaspace_capacity_kB",
            "MU": "metaspace_utilization_kB",
            "CCSC": "compressed_class_space_capacity_kB",
            "CCSU": "compressed_class_space_utilization_kB",
            "PC": "current_permanent_space_capacity_kB",
            "PU": "permanent_space_utilization_kB",
            "YGC": "number_of_young_generation_GC_events",
            "YGCT": "young_generation_garbage_collection_time",
            "FGC": "number_of_stop_the_world_events",
            "FGCT": "full_garbage_collection_time",
            "GCT": "total_garbage_collection_time"
        }

        # Stats coming from -gccapacity
        self.metric_maps_gccapacity = {
            "NGCMN": "minimum_new_generation_capacity_kB",
            "NGCMX": "maximum_new_generation_capacity_kB",
            "NGC": "current_new_generation_capacity_kB",
            "S0C": "current_survivor_space_0_capacity_kB",
            "S1C": "current_survivor_space_1_capacity_kB",
            "EC": "current_eden_space_capacity_kB",
            "OGCMN": "minimum_old_generation_capacity_kB",
            "OGCMX": "maximum_old_generation_capacity_kB",
            "OGC": "current_old_generation_capacity_kB",
            "OC": "current_old_space_capacity_kB",
            "MCMN": "minimum_metaspace_capacity_kB",
            "MCMX": "maximum_metaspace_capacity_kB",
            "MC": "metaspace_capacity_kB",
            "CCSMN": "compressed_class_space_minimum_capacity_kB",
            "CCSMX": "compressed_class_space_maximum_capacity_kB",
            "CCSC": "compressed_class_space_capacity_kB",
            "YGC": "number_of_young_generation_GC_events",
            "FGC": "number_of_stop_the_world_events"
        }

        # Stats coming from -gccause
        self.metric_maps_gccause = {
            "LGCC": "cause_of_last_garbage_collection",
            "GCC": "cause_of_current_garbage_collection"
        }

        # Stats coming from -gcnew
        self.metric_maps_gcnew = {
            "S0C": "current_survivor_space_0_capacity_kB",
            "S1C": "current_survivor_space_1_capacity_kB",
            "S0U": "survivor_space_0_utilization_kB",
            "S1U": "survivor_space_1_utilization_kB",
            "TT": "tenuring_threshold",
            "MTT": "maximum_tenuring_threshold",
            "DSS": "desired_survivor_size_kB",
            "EC": "current_eden_space_capacity_kB",
            "EU": "eden_space_utilization_kB",
            "YGC": "number_of_young_generation_GC_events",
            "YGCT": "young_generation_garbage_collection_time"
        }

        # Stats coming from -compiler
        self.metric_maps_compiler = {
            "Compiled": "number_of_compilation_tasks_performed",
            "Failed": "number_of_compilation_tasks_failed",
            "Invalid": "number_of_compilation_tasks_that_were_invalidated",
            "Time": "time_spent_performing_compilation_tasks",
            "FailedType": "compile_type_of_the_last_failed_compilation",
            "FailedMethod":
            "class_name_and_method_of_the_last_failed_compilation"
        }

        # Stats coming from -class
        ## Note that since "Bytes" appears twice in jstat -class output we need
        ## to differentiate them by colum number
        self.metric_maps_class = {
            "Loaded": "number_of_classes_loaded",
            "Bytes_column2": "number_of_kBs_loaded",
            "Unloaded": "number_of_classes_unloaded",
            "Bytes_column4": "number_of_kBs_unloaded",
            "Time":
            "time_spent_performing_class_loading_and_unloading_operations"
        }

    def _is_executable(self, fpath):
        '''Check if a path both exists and is executable.

        Args:
            fpath (str): The path to the file / program you would like to check

        Returns:
            (bool): True for exists and executable, otherwise False
        '''
        return isfile(fpath) and access(fpath, X_OK)

    def _which(self, program):
        '''Find the path to an executable.

        This method is designed to replicate the functionality of `which`,
        allowing us to find the full path to an executable.

        Args:
            program (:obj:`str`): The program name that you would like to find the
                path to.

        Returns:
            (:obj:`str` or :obj:`None`): string path to the executable or None for does not
                exist or not executable.
        '''
        self.logger.debug(('application=jstatmon environment={env} '
                           'msg=start JStatmonClient._get_java_pids').format(
                               env=self.environment))
        fpath, fname = split(program)
        if fpath:
            if self._is_executable(program):
                return program
        else:
            for path in environ['PATH'].split(pathsep):
                path = path.strip('"')
                executable = join(path, program)
                if self._is_executable(executable):
                    return executable

    def _get_java_pids(self):
        '''Retrieve all running Java process ID's on the local machine.

        This will use pgrep to identify all currently running java process ID's.

        Returns:
            (:obj:`list` of :obj:`str` or None) The list of pids or None if
                none were found
        '''
        self.logger.debug(('application=jstatmon environment={env} '
                           'msg=start JStatmonClient._get_java_pids').format(
                               env=self.environment))
        pgrep_executable = self._which(program='pgrep')
        if pgrep_executable:
            cmd_array = shlex.split(
                '{pgrep} java'.format(pgrep=pgrep_executable))
            p = Popen(
                cmd_array,
                universal_newlines=True,
                shell=False,
                stdout=PIPE,
                stderr=PIPE)
            p.wait()
            stdout, stderr = p.communicate()
            if stderr:
                self.logger.error(stderr)

            pids = stdout.strip().split('\n')
            self.logger.debug(('application=jstatmon environment={env} '
                               'msg=end JStatmonClient._get_java_pids').format(
                                   env=self.environment))
            return pids
        else:
            self.logger.error('failed to retrieve pgrep executable location')
            self.logger.debug(('application=jstatmon environment={env} '
                               'msg=end JStatmonClient._get_java_pids').format(
                                   env=self.environment))
            return None

    def _pid_to_command(self, pid):
        '''Retrieve the command associated with a PID.

        Args:
            pid (int): The process ID you would like the command string for.

        Returns:
            (:obj:`tuple` of :obj:`str`) pid, command, user
        '''
        self.logger.debug(('application=jstatmon environment={env} '
                           'msg=start JStatmonClient._pid_to_command').format(
                               env=self.environment))
        ps_executable = self._which(program='ps')
        if ps_executable:
            cmd_array = shlex.split(
                '{ps} -p {pid} -o command='.format(ps=ps_executable, pid=pid))
            p = Popen(
                cmd_array,
                universal_newlines=True,
                shell=False,
                stdout=PIPE,
                stderr=PIPE)
            p.wait()
            stdout, stderr = p.communicate()
            if stderr:
                self.logger.error(stderr)

            command = stdout.strip()

            cmd_array = shlex.split(
                '{ps} -p {pid} -o user='.format(ps=ps_executable, pid=pid))
            p = Popen(
                cmd_array,
                universal_newlines=True,
                shell=False,
                stdout=PIPE,
                stderr=PIPE)
            p.wait()
            stdout, stderr = p.communicate()
            if stderr:
                self.logger.error(stderr)

            user = stdout.strip()
            self.logger.debug(('application=jstatmon environment={env} '
                               'msg=end JStatmonClient._pid_to_command').format(
                                   env=self.environment))
            return (pid, command, user)
        else:
            self.logger.error(('application=jstatmon environment={env} '
                               'msg=failed to find ps executable').format(
                                   env=self.environment))

            return None

    def _demote(self, user_uid, user_gid):

        def result():
            setgid(user_gid)
            setuid(user_uid)

        return result

    def _jstat_details(self, pid_cmd_user_tuple):
        self.logger.debug(('application=jstatmon environment={env} '
                           'msg=start JStatmonClient._jstat_details').format(
                               env=self.environment))
        jstat_executable = self._which(program='jstat')

        if jstat_executable:
            stats = []
            for option in [
                    '-gc', '-gccapacity', '-gcnew', '-compiler', '-class'
            ]:
                metric_maps = None
                if option == '-gc':
                    self.logger.debug(('application=jstatmon environment={env} '
                                       'msg=-gc option identified').format(
                                           env=self.environment))
                    metric_maps = self.metric_maps_gc
                elif option == '-gccapacity':
                    self.logger.debug(
                        ('application=jstatmon environment={env} '
                         'msg=-gccapacity option identified').format(
                             env=self.environment))
                    metric_maps = self.metric_maps_gccapacity
                elif option == '-gcnew':
                    self.logger.debug(('application=jstatmon environment={env} '
                                       'msg=-gcnew option identified').format(
                                           env=self.environment))
                    metric_maps = self.metric_maps_gcnew
                elif option == '-compiler':
                    self.logger.debug(
                        ('application=jstatmon environment={env} '
                         'msg=-compiler option identified').format(
                             env=self.environment))
                    metric_maps = self.metric_maps_compiler
                elif option == '-gccause':
                    self.logger.debug(('application=jstatmon environment={env} '
                                       'msg=-gccause option identified').format(
                                           env=self.environment))
                    metric_maps = self.metric_maps_gccause
                elif option == '-class':
                    self.logger.debug(('application=jstatmon environment={env} '
                                       'msg=-class option identified').format(
                                           env=self.environment))
                    metric_maps = self.metric_maps_class
                else:
                    self.logger.error('unknown option {opt}'.format(opt=option))

                cmd_array = shlex.split('{jstat} {opt} {pid}'.format(
                    jstat=jstat_executable,
                    opt=option,
                    pid=pid_cmd_user_tuple[0]))

                record = getpwnam(pid_cmd_user_tuple[2])

                p = Popen(
                    cmd_array,
                    universal_newlines=True,
                    preexec_fn=self._demote(record.pw_uid, record.pw_gid),
                    shell=False,
                    stdout=PIPE,
                    stderr=PIPE)
                p.wait()
                stdout, stderr = p.communicate()
                if stderr:
                    self.logger.error(stderr)

                output = stdout.strip()
                values_all = output.split("\n")[1].split()

                # Change stats titles to long names
                titles = output.split("\n")[0].split()

                # Deal with -class special "double Bytes" output
                if option == "-class":
                    titles[1] = "Bytes_column2"
                    titles[3] = "Bytes_column4"

                metrics = [
                    'application=jstatmon', 'environment={env}'.format(
                        env=self.environment),
                    'option={opt}'.format(opt=option), 'user={user}'.format(
                        user=pid_cmd_user_tuple[2]), 'pid={pid}'.format(
                            pid=pid_cmd_user_tuple[0]), 'command={cmd}'.format(
                                cmd=pid_cmd_user_tuple[1])
                ]
                for position, title in enumerate(titles):
                    if title in metric_maps:
                        key = '{opt}_{title}'.format(
                            opt=option[1:], title=metric_maps[title])
                        metrics.append('{key}={value}'.format(
                            key=key, value=values_all[position]))
                    else:
                        self.logger.warning(
                            ('application=jstatmon environment={env} '
                             'msg=item not found in metric map '
                             'option={opt} title={title} '
                             'metric_map={mm}').format(
                                 env=self.environment,
                                 opt=option,
                                 title=title,
                                 mm=metric_maps),)
                stats.append(metrics)
            self.logger.debug(('application=jstatmon environment={env} '
                               'msg=end JStatmonClient._pid_to_command').format(
                                   env=self.environment))
            return stats
        else:
            self.logger.error(('application=jstatmon environment={env} '
                               'msg=failed to find jstat executable').format(
                                   env=self.environment))

            return None
        self.logger.debug(('application=jstatmon environment={env} '
                           'msg=end JStatmonClient._jstat_details').format(
                               env=self.environment))

    def _interpret_jstat(self, metrics):
        self.logger.info(' '.join(metrics))

    def run(self):
        '''Run jstatmon

        This will execute the jstatmon task. Which is composed of the following
        steps:

            1. Identify all java PID's running on the system
            2. Identify the process name for each PID.

        '''
        self.logger.debug(('application=jstatmon environment={env} '
                           'msg=start JStatmonClient.run').format(
                               env=self.environment))

        pids = self._get_java_pids()
        if pids:
            self.logger.debug(('application=jstatmon environment={env} '
                               'msg=retrieved java pids pids={pids}').format(
                                   pids=', '.join(pids), env=self.environment))
            pid_cmd = []
            for pid in pids:
                item = self._pid_to_command(pid=pid)
                if item:
                    pid_cmd.append(item)
                else:
                    self.logger.warning(('application=jstatmon '
                                         'environment={env} '
                                         'msg=Failed to find command / user '
                                         'for PID {pid}').format(
                                             env=self.environment, pid=pid))
            for pc in pid_cmd:
                metrics = self._jstat_details(pc)
                for metric in metrics:
                    self._interpret_jstat(metrics=metric)
        self.logger.debug(('application=jstatmon environment={e} '
                           'msg=end JStatmonClient.run').format(
                               e=self.environment))
