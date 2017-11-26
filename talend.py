#!/usr/bin/python
_VERBOSE = True

import os
import xml.etree.ElementTree as ET


class TalendProject(object):
    def __init__(self, path, is_bare_repo=True, ref="HEAD"):
        self.path = path
        self.is_bare_repo = is_bare_repo
        self.ref = ref
        self.git_tags = None
        self.author = None
        self.jobs = {}
        # job name -> job version list (JOB_NAME: {LATEST: version#, v1: path, ...})
        self.versions = {}
        self.talend_version = None
        self.project_name = None
        self._load()

    def __repr__(self):
        return "Talend project with {0} jobs located in {1}".format(
            len(self.versions), self.path)

    def _load(self):
        # get all .item's
        items = self._get_item_paths()

        # get tags (NOTE: this won't include the CURRENT deployment tag if this is a deployment request!)
        self.git_tags = self._get_tags()

        self.author = self._get_author()

        # get Talend version
        stream = GitFile(self.path, "talend.project", self.ref, self.is_bare_repo).get_stream()
        root = ET.parse(stream).getroot()
        if stream.close() is not None:
            raise IOError("Problem reading talend.project file")

        if 'productVersion' in root[0].attrib:
            self.talend_version = root[0].attrib['productVersion']
        else:
            raise ParseError("Unknown Talend version. Cannot parse!")

        if 'technicalLabel' in root[0].attrib:
            self.project_name = root[0].attrib['technicalLabel']
        else:
            raise ParseError("Couldn't find Talend project name. XML corrupt?")


        for i in items:
            name = i.split('/')[-1][:-9]
            version = float(i.split('_')[-1][:-5])

            if name in self.versions:
                if 'LATEST' in self.versions[name]:
                    # check if this version is greater
                    if version > self.versions[name]['LATEST']:
                        self.versions[name]['LATEST'] = version
                self.versions[name][version] = i
            else:
                self.versions[name] = {version: i, 'LATEST': version}


            self.jobs[i] = TalendJob(name, i, GitFile(self.path, i, self.ref, self.is_bare_repo), version=version, parse=False)
            #verbose("Initialized job " + name)

    def __getitem__(self, job_name):
        # gets the latest version of job
        if 'item' in job_name:
            return self.jobs[job_name]
        else:
            return self.jobs[self.versions[job_name][self.versions[job_name]['LATEST']]]

    def __iter__(self):
        for item in self.versions:
            yield self[item]

    def list_jobs(self):
        for j in self.versions:
            print(j)
            print("\t" + str([v for v in self.versions[j] if not v == 'LATEST']))
            print("\tLATEST: " + str(self.versions[j]['LATEST']))

    def get_all_tables(self):
        tables = set()
        for j in self:
            j.parse()
            tables |= j.tables
        return list(tables)

    def _get_item_paths(self):
        ret = []
        git_dir = self.path if self.is_bare_repo else os.path.join(self.path, ".git")

        # get list from git
        git_stream = os.popen("git --git-dir \"{0}\" ls-tree --full-tree -r {1}".format(git_dir, self.ref))
        lines = git_stream.readlines()

        if git_stream.close() is not None:
            raise IOError("Error reading item list!")

        ret = [x.split()[3] for x in lines if "process" in x and ".item" in x]

        return ret

    def _get_tags(self):
        git_dir = self.path if self.is_bare_repo else os.path.join(self.path, ".git")

        # get list from git
        git_stream = os.popen("git --git-dir \"{0}\" tag --points-at {1}".format(git_dir, self.ref))

        try:
            lines = git_stream.readlines()
        except IOError:
            return []

        if git_stream.close() is not None:
            return []

        return [l.strip() for l in lines]

    def _get_author(self):
        git_dir = self.path if self.is_bare_repo else os.path.join(self.path, ".git")

        # get list from git
        git_stream = os.popen("git --git-dir \"{0}\" show -s --format=\"%aN <%aE>\" {1}".format(git_dir, self.ref))

        try:
            lines = git_stream.readlines()
        except IOError:
            return "Error reading author"

        if git_stream.close() is not None:
            return "Error reading author"

        if lines is None or len(lines) == 0:
            return "Error reading author"

        return lines[0].strip()


    def review(self, job, children=True):
        # format:
        # {error name: {job name: [{cmpnt name: '', msg: ''}, ...], ...}, ...}
        ret = {}

        if job:
            ret = self[job].review()
            if children:
                for child in self[job].children:
                    child_errs = self.review(child)
                    for err in child_errs:
                        if not err in ret:
                            ret[err] = child_errs[err]
                        else:
                            ret[err].update(child_errs[err]) # {job name: []}

        else:
            # check all jobs
            for j in self:
                ret[j.name] = j.review()
        return ret

    def dump_sql(self, job, children=True, header=True):
        ret = []

        if job and children:
            ret.append(self[job].dump_sql())
            for child in self[job].children:
                ret.append(self.dump_sql(child, header=header))
        else:
            # dump all sql
            for j in self:
                ret.append(j.dump_sql(header=header))

        return "\n".join(ret)

    def dump_sql_structured(self):
        ret = {}
        for j in self:
            ret[j.nickname] = j.dump_sql_structured()
        return ret


    def review_with_info(self, job, children=True):
        dbinfo = self.get_database_info(job, children)
        dbinfo = [{'host': x[0], 'database': x[1], 'user': x[2]} for x in set(dbinfo)]
        return {
                'project_name':     self.project_name,
                'author':           self.author,
                'git_tags':         self.git_tags,
                'talend_version':   self.talend_version,
                'job_name':         self[job].name,
                'repo_path':        self.path,
                'commit_ref':       self.ref,
                'errors':           self.review(job, children),
                'dbinfo':           dbinfo,
                'context':          self.get_merged_context(job),
                'tree':             self.tree_view(self[job].name),
                'job_version':      self[job].version
               }

    def get_merged_context(self, job):
        ctx = {}
        for context in self[job].context:
            ctx[context] = self[job].context[context]

        for child in self[job].children:
            child_contexts = self.get_merged_context(child)

            for context in child_contexts:
                if context in ctx:
                    for param in child_contexts[context]:
                        if param not in ctx[context]:
                            ctx[context][param] = child_contexts[context][param]
                else:
                    ctx[context] = child_contexts[context]
        return ctx


    def get_database_info(self, job, children=True):
        ret = []
        if job:
            ret += self[job].get_database_info()
            if children:
                for child in self[job].children:
                    if child in ret:
                        # possible infinite loop.
                        continue
                    else:
                        ret += self.get_database_info(child)
        else:
            # check all jobs
            for j in self:
                ret += j.get_database_info()

        return ret


    def get_master_jobs(self):
        masters = {j: True for j in self.versions}
        for j in self.versions:
            if not self[j].parsed:
                self[j].parse()

            for child in self[j].children:
                masters[child] = False

        return [j for j in masters if masters[j]]

    def tree_view(self, root=None):
        if not root in self.versions:
            return {}
        if root:
            tree = {root: {}}
            for child in self[root].children:
                tree[root][child] = self.tree_view(child)[child]
        else:
            tree = {}
            masters = self.get_master_jobs()
            for master in masters:
                tree.update(self.tree_view(master))
        return tree




class TalendJob(object):
    def __init__(self, name, path, gitfile, version=None, parse=True):
        self.name = name
        self.path = path
        self.version = version
        self.nodes = {}
        self.connections = {}
        self.context = {}
        self.children = []
        self.gitfile = gitfile
        self.use_teradata = None
        self.use_files = False
        self.parsed = False
        self.tables = set()
        if parse: self.parse() 
    def __repr__(self):
        if self.parsed:
            return "Talend job '{0}' with {1} components and {2} sub jobs".format(self.name, len(self.nodes), len(self.children))
        else:
            return "Talend job '{0}' from {1} (not parsed)".format(self.name, self.gitfile)

    def __getitem__(self, node):
        if not self.parsed:
            self.parse()

        return self.nodes[node]

    def __iter__(self):
        if not self.parsed:
            self.parse()

        for node in self.nodes:
            yield self[node]


    def parse(self):
        if self.parsed:
            return

        stream = self.gitfile.get_stream()
        root = ET.parse(stream).getroot()
        if stream.close() is not None:
            raise IOError("Problem reading file " + self.gitfile)

        self.use_teradata = False
        self.use_files = False

        # get all components, connections, and context parameters
        for node in root:
            if 'node' in node.tag:
                params = {}

                for element in node:
                    if 'elementParameter' in element.tag and 'value' in element.attrib:
                        params[element.attrib['name']] = element.attrib['value']

                if 'ACTIVATE' in params and params['ACTIVATE'] == 'false':
                    continue

                params['_componentName'] = node.attrib['componentName']

                if not self.use_teradata and 'teradata' in params['_componentName'].lower():
                    self.use_teradata = True

                if not self.use_files and 'file' in params['_componentName'].lower():
                    self.use_files = True

                self.nodes[params['UNIQUE_NAME']] = params

                if node.attrib['componentName'] == 'tRunJob':
                    self.children.append(params['PROCESS'])

                # tables
                dbname = params.get('DBNAME', '')
                table = params.get('TABLE')
                if table:
                    self.tables.add((dbname + '.' + table).replace('"',''))

            elif 'connection' in node.tag:
                connection_type = node.attrib['connectorName']
                connection_activated = False
                connection_name = None
                for element in node:
                    if 'elementParameter' in element.tag:
                        if element.attrib['name'] == 'ACTIVATE':
                            connection_activated = element.attrib['value']
                        elif element.attrib['name'] == 'UNIQUE_NAME':
                            connection_name = element.attrib['value']
                if connection_activated == "true":
                    self.connections[connection_name] = connection_type
            elif 'context' in node.tag:
                context_name = node.attrib['name']
                params = {}
                for context_param in node:
                    params[context_param.attrib['name']] = context_param.attrib['value']
                self.context[context_name] = params




        self.parsed = True

    # static review parameters:
    check_params = {'DIE_ON_ERROR':              'true',
                    'DIE_ON_CHILD_ERROR':        'true',
                    'USE_INDEPENDENT_PROCESS':   'false',
                    'TRANSMIT_ORIGINAL_CONTEXT': 'true',
                    'TRANSMIT_WHOLE_CONTEXT':    'true'}

    # Static review parameters for tFile components are separate since there is posible ambiguity
    file_params =  {'CREATEDIR':        'false',
                    'CREATE':           'false',
                    'MKDIR':            'false',
                    'CREATE_DIRECTORY': 'false'}
                #   'FAILON':           'true'} # for tFileDelete

    edw_params   = {'HOST': ['context.EDW_HOST'],
                    'USER': ['context.EDW_USER'],
                    'PASS': ['context.EDW_PASS', '4D9onkGJm3fNdrQLmTZZevT3q6F0Z4TqEncrypt']}

    edw_context  = ['EDW_HOST', 'EDW_USER', 'EDW_PASS']

    def review(self):
        # Review will report on:
        #   DIE_ON_ERROR = true
        #   DIE_ON_CHILD_ERROR = true
        #   TODO: Report on file paths
        if not self.parsed:
            self.parse()

        # review results are stored as {error name: {job name: [{component: component, message: mesg}, ...]}}
        results = {k: {self.name: []} for k in TalendJob.check_params}
        results.update({k: {self.name: []} for k in TalendJob.edw_params})
        results.update({k: {self.name: []} for k in TalendJob.file_params})
        results['ON_COMPONENT_ERROR'] = {self.name: []}
        results['CONTEXT'] = {self.name: []}
        results['QUERY'] = {self.name: []}

        for node in self.nodes:
            if 'ACTIVATE' in self.nodes[node] and self.nodes[node]['ACTIVATE'] == 'false':
                # node disabled, skip
                continue

#           if 'QUERY' in self.nodes[node] and "--" in self.nodes[node]['QUERY']:
#               results['QUERY'][self.name].append({
#                       'component': node,
#                       'message': "Please use multiline comments '/* ... */' instead of single line '--'",
#                       })

            for param in TalendJob.check_params:
                if param in self.nodes[node] and self.nodes[node][param] != TalendJob.check_params[param]:
                    results[param][self.name].append({
                              'component': node,
                              'message': "Value '{0}' must be set to '{1}' (actual: '{2}')".format(
                                        param,
                                        TalendJob.check_params[param],
                                        self.nodes[node][param])})

            # check EDW context variables
            if self.use_teradata and 'teradata' in self.nodes[node]['_componentName'].lower() and (not 'USE_EXISTING_CONNECTION' in self.nodes[node] or self.nodes[node]['USE_EXISTING_CONNECTION'] == 'false'):
                for param in TalendJob.edw_params:
                    if param in self.nodes[node] and not self.nodes[node][param] in TalendJob.edw_params[param]:
                        msg = self.nodes[node][param]
                        if 'Encrypt' in msg:
                            msg = '*ENCRYPTED*'

                        results[param][self.name].append({
                                  'component': node,
                                  'type': self.nodes[node]['TYPE'],
                                  'message': "Value '{0}' must be set to '{1}' (actual: '{2}')".format(
                                            param,
                                            TalendJob.edw_params[param][0],
                                            msg)})

            # Check file parameters
            if self.use_files:
                for param in TalendJob.file_params:
                    if param in self.nodes[node] and self.nodes[node][param] != TalendJob.file_params[param]:
                        results[param][self.name].append({
                              'component': node,
                              'message': "Value '{0}' must be set to '{1}' (actual: '{2}')".format(
                                        param,
                                        TalendJob.file_params[param],
                                        self.nodes[node][param])})

        # check context
        if self.use_teradata:
            for context in self.context:
                ctx_keys = self.context[context].keys()
                break


            for ctx_key in TalendJob.edw_context:
                if not ctx_key in ctx_keys:
                    results['CONTEXT'][self.name].append({'component': 'Context',
                                       'message': "Teradata job detected but missing context parameter '{0}'".format(ctx_key)})

        # check connections
        for conn in self.connections:
            if (self.connections[conn] == "COMPONENT_ERROR" or
                    self.connections[conn] == "ON_COMPONENT_ERROR"):
                results['ON_COMPONENT_ERROR'][self.name].append({
                    'component': conn,
                    'message': "'OnComponentError' trigger depreciated. Use 'if' trigger instead."})

        # Will return {} if no errors
        return {e: results[e] for e in results if results[e][self.name]}

    def get_database_info(self):
        if not self.parsed:
            self.parse()

        info = [] # [host, dbname]
        for node in self:
            if "USE_EXISTING_CONNECTION" in node and node['USE_EXISTING_CONNECTION'] == 'true':
                continue

            host = node['HOST'].upper() if 'HOST' in node else None
            dbname = node['DBNAME'].strip('"').upper() if 'DBNAME' in node else None
            user = node['USER'] if 'USER' in node else None

            if host and dbname:
                info.append((host, dbname, user))

        return info

    def dump_sql(self, header=False):
        if not self.parsed:
            self.parse()

        ret = []
        for node in self.nodes:
            if ('ACTIVATE' in self.nodes[node] and
                    self.nodes[node]['ACTIVATE'] == 'false'):
                continue

            if 'QUERY' in self.nodes[node]:
                if 'DBNAME' in self.nodes[node]:
                    schema = self.nodes[node]['DBNAME']
                else:
                    schema = '???'

                # header
                if header:
                    ret.append("-- Job Name:\t{0}\n-- Component:\t{1}\n-- Schema:\t{2}".format(self.name,
                                                                                                node, schema))

                # text
                ret.append(self.nodes[node]['QUERY'].replace('"', ''))

        return "\n".join(ret)

    def dump_sql_structured(self, header=False):
        if not self.parsed:
            self.parse()

        ret = {}
        for node in self.nodes:
            if ('ACTIVATE' in self.nodes[node] and
                    self.nodes[node]['ACTIVATE'] == 'false'):
                continue

            if 'QUERY' in self.nodes[node]:
                if 'DBNAME' in self.nodes[node]:
                    schema = self.nodes[node]['DBNAME']
                else:
                    schema = '???'

                # text
                ret[node] = (schema, self.nodes[node]['QUERY'].replace('"', ''))

        return ret


class GitFile(object):
    def __init__(self, parent, child, ref="MASTER", bare=True):
        self.child = child
        self.ref = ref
        if bare == False:
            self.parent = os.path.join(parent, ".git")
        else:
            self.parent = parent

    def get_stream(self):
        if self.ref is None:
            return open(os.path.join(self.parent, self.child), 'r')
        else:
            return os.popen("git --git-dir \"{0}\" show {1}:{2}".format(
                self.parent, self.ref, self.child))

    def __repr__(self):
        return "{0}/{2} [{1}]".format(self.parent, self.ref, self.child)


def verbose(mesg):
    if _VERBOSE == True:
        print(mesg)

class ParseError(Exception):
    pass

