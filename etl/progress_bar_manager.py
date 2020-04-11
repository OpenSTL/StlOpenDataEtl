import logging
import enlighten



# class ProgressBarManager:
#    __instance = None
#    @staticmethod
#    def getInstance():
#       """ Static access method. """
#       if ProgressBarManager.__instance == None:
#          logging.debug('Delay creation of ProgressBarManager object...')
#          ProgressBarManager()
#       return ProgressBarManager.__instance
#
#    def __init__(self):
#       """ Virtually private constructor. """
#       if ProgressBarManager.__instance != None:
#          raise Exception("This class is a singleton!")
#       else:
#          logging.debug('Creating ProgressBarManager object...')
#          ProgressBarManager.__instance = enlighten.get_manager()

class ProgressBarManager(object):
    _instance = None

    class __ProgressBarManager:
        """ private class """
        def __init__(self):
            """ private constructor """
            self._manager = enlighten.get_manager()

        def add_pbar(self, job_count, name, unit):
            pbar = self._manager.counter(total=job_count, desc=name, unit=unit)
            return pbar

        # def __str__(self):
        #     logging.debug('Calling private __str__')
        #     return self._manager

    def __new__(cls):
        """
        Arguments:
        Creates and returns ProgressBar object if it doesn't exist;
        Returns ProgressBar object if it exists from previous initialization.
        note: __new__ called at instances creation before __init__
        """
        if not ProgressBarManager._instance:
            ProgressBarManager._instance = ProgressBarManager.__ProgressBarManager()
            logging.debug('Creating ProgressBarManager object...')
        else:
            logging.debug('Using existing ProgressBarManager object...')
        return ProgressBarManager._instance


    # def update(self):
    #     self.pbar.update()

    def __getattr__(self, name):
        logging.debug('Calling __getattr__')
        return getattr(self._instance, name)

    # def __setattr__(self, name):
    #     logging.debug('Calling __setattr__')
    #     return setattr(self.instance, name)
