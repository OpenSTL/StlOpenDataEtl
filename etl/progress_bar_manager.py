import logging
import enlighten


class ProgressBarManager(object):
    _instance = None

    class __ProgressBarManager:
        """ private class """
        _pbar_dict = dict()

        def __init__(self):
            """ private constructor """
            self._manager = enlighten.get_manager()

        def __str__(self):
            return f'a {self._manager} , {self._pbar_dict}'

        def __repr__(self):
            return f'{self.__class__.__name__}('f'{self._manager!r}, {self._pbar_dict!r})'

        def add_pbar(self, job_count, name, unit):
            """ add progress bar to manager """
            pbar = self._manager.counter(total=job_count, desc=name, unit=unit)
            self._pbar_dict[name] = pbar
            return pbar

        def get_pbar(self, name):
            """ get progress bar to manager using name"""
            if name not in self._pbar_dict:
                logging.debug('ProgressBarManager cannot find progress bar with the name %s', name)
                return None
            return self._pbar_dict[name]

    def __new__(cls):
        """
        Creates and returns ProgressBar object not initialized;
        Returns ProgressBar object if initialized previously.
        """
        if not ProgressBarManager._instance:
            ProgressBarManager._instance = ProgressBarManager.__ProgressBarManager()
            logging.debug('Creating new ProgressBarManager instance (%s)',
                          hex(id(ProgressBarManager._instance)))
        else:
            logging.debug('Using existing ProgressBarManager instance (%s)',
                          hex(id(ProgressBarManager._instance)))
        return ProgressBarManager._instance
