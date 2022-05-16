# This program contains a class that represents a point stat object. It is the typical layout for point stats

class PointStat:

    def __init__(self, year: int, point_category: int):
        self.year = year
        self._points = point_category
        self._pts_spent = 0
        self._applicants = 0
        self._successes = 0
        self._percent_successful = 0

    def set_apps(self, applicants):
        self._applicants = applicants
        
        if self._points == 0:
            self._pts_spent = applicants
        else:
            self._pts_spent = applicants * (self._points ** 2)

    def set_successes(self, num_successful):
        self._successes = num_successful

    def set_perc_success(self):
        if self._applicants != 0:
            self._percent_successful = round(self._successes / self._applicants * 100, 1)

    def convert_to_dict(self):
        return {
            'year': self.year,
            'points': self._points,
            'applicants': self._applicants,
            'pts spent': self._pts_spent,
            'successes': self._successes,
            '% success': self._percent_successful
        }
