# encoding=utf8

from resource_management import *
from resource_management import *

from zeppelin import zeppelin

class Client(Script):
    def configure(self, env):
        import params
        env.set_params(params)
        pass

    def start(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        self.configure(env)
        pass

    def stop(self, env, upgrade_type=None):
        import params
        env.set_params(params)
        pass

    def status(self, env):
        raise ClientComponentHasNoStatus()


if __name__ == "__main__":
    Client().execute()
