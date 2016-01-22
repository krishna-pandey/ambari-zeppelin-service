# encoding=utf8

import glob
import grp
import os
import pwd
import sys
import time
from resource_management import *

reload(sys)
sys.setdefaultencoding('utf8')


class Master(Script):
    def install(self, env):

        import params

        env.set_params(params)

        Execute('find ' + params.service_packagedir + ' -iname "*.sh" | xargs chmod +x')

        # Create user and group if they don't exist
        self.create_linux_user(params.zeppelin_user, params.zeppelin_group)
        # self.create_hdfs_user(params.zeppelin_user, params.spark_jar_dir)

        # remove /usr/hdp/current/zeppelin if already exists
        Execute('rm -rf ' + params.zeppelin_dir, ignore_failures=True)

        # if on CentOS and python packages specified, install them
        if params.install_python_packages:
            distribution = platform.linux_distribution()[0].lower()
            version = str(platform.linux_distribution()[1])
            Execute('echo platform.linux_distribution:' + platform.linux_distribution()[0] + '+' +
                    platform.linux_distribution()[1] + '+' + platform.linux_distribution()[2])

            Execute('echo distribution is: ' + distribution)
            Execute('echo version is: ' + version)

            # not sure if we really need these.
            if distribution.startswith('centos'):
                if version.startswith('7'):
                    Execute('echo Installing python packages for Centos 7')
                    Execute('yum install -y epel-release')
                    Execute('yum install -y python-pip python-matplotlib python-devel numpy scipy '
                            'python-pandas gcc gcc-c++')
                    Execute('pip install --user --install-option="--prefix=" -U scikit-learn')
                if version.startswith('6'):
                    Execute('echo Installing python packages for Centos 6')
                    Execute('yum install -y python-devel python-nose python-setuptools gcc '
                            'gcc-gfortran gcc-c++ blas-devel lapack-devel atlas-devel')
                    Execute('easy_install pip', ignore_failures=True)
                    Execute('pip install numpy scipy pandas scikit-learn')

        # User selected option to use prebuilt zeppelin package


        Execute('yum install -y zeppelin')
        Execute('rm -rf /usr/hdp/current/zeppelin')
        Execute('ln -s /usr/hdp/2.4.1.0-130/zeppelin /usr/hdp/current/zeppelin')
        Execute('chown -R zeppelin:hadoop /usr/hdp/current/zeppelin/')

        # create the log, pid, zeppelin dirs
        Directory([params.zeppelin_pid_dir, params.zeppelin_log_dir, params.zeppelin_dir],
                  owner=params.zeppelin_user,
                  group=params.zeppelin_group,
                  recursive=True
                  )

        File(params.zeppelin_log_file,
             mode=0644,
             owner=params.zeppelin_user,
             group=params.zeppelin_group,
             content=''
             )

        Execute('echo spark_version:' + params.spark_version + ' detected for spark_home: '
                + params.spark_home + ' >> ' + params.zeppelin_log_file)

        # update the configs specified by user
        self.configure(env)

        self.install_maven()
        self.install_git()

        # run setup_snapshot.sh
        Execute(format("{service_packagedir}/scripts/setup_snapshot.sh {zeppelin_dir} "
                       "{hive_metastore_host} {hive_metastore_port} {zeppelin_host} {zeppelin_port}"
                       " {setup_view}  >> {zeppelin_log_file}"),
                user=params.zeppelin_user)

        # if zeppelin installed on ambari server, copy view jar into ambari views dir
        if params.setup_view:
            if params.ambari_host == params.zeppelin_internalhost and not os.path.exists(
                    '/var/lib/ambari-server/resources/views/zeppelin-view-1.0-SNAPSHOT.jar'):
                Execute('echo "Copying zeppelin view jar to ambari views dir"')
                Execute('cp /var/lib/zeppelin/zeppelin-view/target/*.jar '
                        '/var/lib/ambari-server/resources/views')

        Execute('cp ' + params.zeppelin_dir
                + '/interpreter/spark/dep/zeppelin-spark-dependencies-*.jar /tmp',
                user=params.zeppelin_user)

    def create_linux_user(self, user, group):
        try:
            pwd.getpwnam(user)
        except KeyError:
            Execute('adduser ' + user)
        try:
            grp.getgrnam(group)
        except KeyError:
            Execute('groupadd ' + group)

    def create_hdfs_user(self, user, spark_jar_dir):
        Execute('hadoop fs -mkdir -p /user/' + user, user='hdfs', ignore_failures=True)
        Execute('hadoop fs -chown ' + user + ' /user/' + user, user='hdfs')
        Execute('hadoop fs -chgrp ' + user + ' /user/' + user, user='hdfs')

        Execute('hadoop fs -mkdir -p ' + spark_jar_dir, user='hdfs', ignore_failures=True)
        Execute('hadoop fs -chown ' + user + ' ' + spark_jar_dir, user='hdfs')
        Execute('hadoop fs -chgrp ' + user + ' ' + spark_jar_dir, user='hdfs')

    def configure(self, env):
        import params
        import status_params
        env.set_params(params)
        env.set_params(status_params)

        # write out zeppelin-site.xml
        XmlConfig("zeppelin-site.xml",
                  conf_dir=params.conf_dir,
                  configurations=params.config['configurations']['zeppelin-config'],
                  owner=params.zeppelin_user,
                  group=params.zeppelin_group
                  )
        # write out zeppelin-env.sh
        env_content = InlineTemplate(params.zeppelin_env_content)
        File(format("{params.conf_dir}/zeppelin-env.sh"), content=env_content,
             owner=params.zeppelin_user, group=params.zeppelin_group)  # , mode=0777)

    def stop(self, env):
        import params
        # self.configure(env)
        Execute(params.zeppelin_dir + '/bin/zeppelin-daemon.sh stop >> ' + params.zeppelin_log_file,
                user=params.zeppelin_user)

    def start(self, env):
        import params
        import status_params
        self.configure(env)

        first_setup = False

        # cleanup temp dirs
        note_osx_dir = params.notebook_dir + '/__MACOSX'
        if os.path.exists(note_osx_dir):
            Execute('rm -rf ' + note_osx_dir)

        if glob.glob('/tmp/zeppelin-spark-dependencies-*.jar') and os.path.exists(
                glob.glob('/tmp/zeppelin-spark-dependencies-*.jar')[0]):
            first_setup = True
            self.create_hdfs_user(params.zeppelin_user, params.spark_jar_dir)
            Execute('hadoop fs -put /tmp/zeppelin-spark-dependencies-*.jar ' + params.spark_jar,
                    user=params.zeppelin_user, ignore_failures=True)
            Execute('rm /tmp/zeppelin-spark-dependencies-*.jar')

        Execute(params.zeppelin_dir + '/bin/zeppelin-daemon.sh start >> '
                + params.zeppelin_log_file, user=params.zeppelin_user)
        pidfile = glob.glob(status_params.zeppelin_pid_dir
                            + '/zeppelin-' + params.zeppelin_user + '*.pid')[0]
        Execute('echo pid file is: ' + pidfile, user=params.zeppelin_user)
        contents = open(pidfile).read()
        Execute('echo pid is ' + contents, user=params.zeppelin_user)

        # if first_setup:
        import time
        time.sleep(5)
        self.update_zeppelin_interpreter()

    def status(self, env):
        import status_params
        env.set_params(status_params)

        pid_file = glob.glob(status_params.zeppelin_pid_dir + '/zeppelin-'
                             + status_params.zeppelin_user + '*.pid')[0]
        check_process_status(pid_file)

    def install_maven(self):
        # for centos/RHEL 6/7 maven repo needs to be installed
        distribution = platform.linux_distribution()[0].lower()
        if distribution.startswith('centos') \
                or distribution.startswith('red hat') \
                        and not os.path.exists('/etc/yum.repos.d/epel-apache-maven.repo'):
            Execute('curl -o /etc/yum.repos.d/epel-apache-maven.repo '
                    'https://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo'
                    )
            Execute('yum install -y apache-maven')

    def install_git(self):
        distribution = platform.linux_distribution()[0].lower()
        if distribution.startswith('centos') \
                or distribution.startswith('red hat'):
            Execute('yum install -y git')

    def update_zeppelin_interpreter(self):
        import params
        import json, urllib2
        zeppelin_int_url = 'http://' + params.zeppelin_host + ':' + str(
                params.zeppelin_port) + '/api/interpreter/setting/'

        # fetch current interpreter settings for spark, hive, phoenix
        data = json.load(urllib2.urlopen(zeppelin_int_url))
        print data
        for body in data['body']:
            if body['group'] == 'spark':
                sparkbody = body
            elif body['group'] == 'hive':
                hivebody = body
            elif body['group'] == 'phoenix':
                phoenixbody = body

        # if hive installed, update hive settings and post to hive interpreter
        if (params.hive_server_host):
            hivebody['properties'][
                'hive.hiveserver2.url'] = 'jdbc:hive2://' + params.hive_server_host + ':10000'
            self.post_request(zeppelin_int_url + hivebody['id'], hivebody)

        # if hbase installed, update hbase settings and post to phoenix interpreter
        if (params.zookeeper_znode_parent and params.hbase_zookeeper_quorum):
            phoenixbody['properties'][
                'phoenix.jdbc.url'] = "jdbc:phoenix:" + params.hbase_zookeeper_quorum + ':' + params.zookeeper_znode_parent
            self.post_request(zeppelin_int_url + phoenixbody['id'], phoenixbody)

    def post_request(self, url, body):
        import json, urllib2
        encoded_body = json.dumps(body)
        req = urllib2.Request(str(url), encoded_body)
        req.get_method = lambda: 'PUT'
        try:
            response = urllib2.urlopen(req, encoded_body).read()
        except urllib2.HTTPError, error:
            print 'Exception: ' + error.read()

        jsonresp = json.loads(response.decode('utf-8'))
        print jsonresp['status']


if __name__ == "__main__":
    Master().execute()
