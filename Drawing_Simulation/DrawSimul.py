# This file determines draw odds by performing a mock drawing for various scenarios
import random


class ApplicantNode:
    """A node for the linked list. The node contains the point category and the adjusted number of applicants for
    that point cat (after accounting for point squaring)."""

    def __init__(self, app_id: int, point_val: int):
        self.app_id = app_id
        self.point_val = point_val

        if self.point_val == 0:
            self.apps = 1
        else:
            self.apps = (self.point_val ** 2)


class AppBag:
    """A data structure that represents a random bag of applicants."""

    def __init__(self):
        self.bag = []
        self.size = 0

    def add_to_bag(self, applicant: ApplicantNode):
        """Adds an applicant to the bag. NOTE - bag must be randomized by calling randomize_bag after adding all
        applicants to the bag."""
        self.bag.append(applicant)
        self.size += 1

    def randomize_bag(self):
        """Randomizes the applicants in the bag."""
        # loop through the bag list, for every index create a new random index between 0 and list length. Swap the
        # current element with the random index.
        random.seed()
        for curr_index in range(self.size):
            rand_index = random.randint(0, self.size - 1)
            curr_applicant = self.bag[curr_index]
            swap_applicant = self.bag[rand_index]
            self.bag[rand_index] = curr_applicant
            self.bag[curr_index] = swap_applicant

    def draw_from_bag(self, index: int):
        """Returns an applicant ID from the bag. Index should be randomized when drawing from the bag to have a
        true random draw."""
        return self.bag[index]


def analyze_trends(app_array: list):
    """This function requires a 2D array with rows acting as years and columns acting as point values. The numbers in
    the 2D array must be the number of applications in that point category. The function analyzes the trends and
    determines what next years drawing might look like. It returns a list (with columns as point cats) with the
    anticipated number of applicants for that point category in the next year."""
    pass


class DrawSimul:

    def __init__(self, expected_apps: list, tag: str, num_tags: int):
        self.tag = tag
        self.num_tags = num_tags
        self.expected_apps = expected_apps

        # attribute to throw the applicant IDs into for the random drawing
        self.apps_bag = AppBag()

        # attribute to hold what IDs have been drawn from the bag already so we don't double draw
        self.draw_ids = set()

        # attribute to hold the results of the dwgs (number of applicants selected in each point cat)
        self.dwg_results = [0]*21

    def draw_rand_id(self):
        """Draws a random ID from the apps_bag. Keeps drawing until an ID that hasn't been drawn gets drawn."""
        random.seed()
        rand_index = random.randint(0, self.apps_bag.size - 1)
        rand_applicant = self.apps_bag.draw_from_bag(rand_index)

        if rand_applicant.app_id in self.draw_ids:
            return self.draw_rand_id()

        self.dwg_results[rand_applicant.point_val] += 1
        self.draw_ids.add(rand_applicant.app_id)

    def add_applicants_to_bag(self):
        """Adds all of the applicants to the bag for drawing."""
        app_id = 0
        for i, applicants in enumerate(self.expected_apps):
            for app in range(applicants):
                new_app = ApplicantNode(app_id, i)
                while new_app.apps > 0:
                    self.apps_bag.add_to_bag(new_app)
                    new_app.apps -= 1
                app_id += 1

        for i in range(2):
            self.apps_bag.randomize_bag()

    def run_drawing(self):
        """performs a drawing"""
        # add all the applicants to the bag
        self.add_applicants_to_bag()

        # draw random applicants from the bag until you hit the number of tags
        # NOTE - self.draw_rand_id retries automatically if you re-draw a certain person
        for _ in range(self.num_tags):
            self.draw_rand_id()

        return self.dwg_results
