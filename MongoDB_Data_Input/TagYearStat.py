# This program contains a class that represents a tag's year stat object. It is the typical layout for year stats

class YearStat:

    def __init__(self, year: int):
        self.year = year
        self._applicants = 0
        self._successes = 0
        self._percent_successful = 0
        self._pts_spent = 0
        self._avg_pts_per_app = 0

    def set_apps(self, applicants):
        self._applicants = applicants
        
    def set_pts_spent(self, pts_spent):
        self._pts_spent = pts_spent

    def set_successes(self, num_successful):
        self._successes = num_successful

    def set_perc_success(self):
        if self._applicants != 0:
            self._percent_successful = round(self._successes / self._applicants * 100, 1)

    def set_avg_pts_per_app(self, total_weighted_pts):
        """Determines the weighted average of the points spent during the application. Requires an input that is the "total weighted
        points", which is just the point category * number of applicants in that category summed over all the point categories."""
        if self._applicants != 0:
            self._avg_pts_per_app = round(total_weighted_pts / self._applicants * 100, 1)

    def convert_to_dict(self):
        return {
            'year': self.year,
            'applicants': self._applicants,
            'pts spent': self._pts_spent,
            'successes': self._successes,
            '% success': self._percent_successful,
            'avg pt per app': self._avg_pts_per_app
        }
