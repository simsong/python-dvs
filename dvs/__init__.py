import time


class DVS():
    def __init__(self, *args, **kwargs):
        """Start a DVS transaction"""
        self.commit = kwargs
        self.t0     = time.time()
        pass

    def add(self, which, *args, **kwargs):
        """Basic method for adding an object to one of the lists """
        assert which in [COMMIT_BEFORE, COMMIT_METHOD, COMMIT_AFTER]
        if which not in self.commit:
            self.commit[which] = list()


    def add_before(self, *args, **kwargs):
        return self.add(COMMIT_BEFORE, *args, **kwargs)

    def add_method(self, *args, **kwargs):
        return self.add(COMMIT_METHOD, *args, **kwargs)

    def add_after(self, *args, **kwargs):
        return self.add(COMMIT_AFTER, *args, **kwargs)


"""
TODO: Take logic in dvs.py/do_commit_send and move here.
TODO: remake dvs.py so that it uses the DVS module.
"""
assert False
