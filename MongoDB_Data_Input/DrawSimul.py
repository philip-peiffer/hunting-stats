# This file determines draw odds by performing a mock drawing for various scenarios
import random


class PointCatNode:
    """A node for the linked list. The node contains the point category and the adjusted number of applicants for
    that point cat (after accounting for point squaring)."""

    def __init__(self, point_val: int, apps: int):
        self.prev = None
        self.next = None
        self.apps = apps
        self.point_val = point_val

        if self.point_val == 0:
            self.adj_apps = self.apps
        else:
            self.adj_apps = (self.point_val ** 2) * self.apps


class PointCatLinkList:
    """A linked list with each node representing a point category."""

    def __init__(self):
        self.head = None

    def add_node(self, point_node: PointCatNode):
        """Adds a node to the start of the linked list."""
        if self.head is None:
            self.head = point_node
        else:
            old_head = self.head
            self.head = point_node
            self.head.next = old_head
            old_head.prev = self.head

    def delete_node(self, point_node: PointCatNode):
        """Deletes a node from the linked list."""
        prev_node = point_node.prev

        # if prev_node is none, we're deleting the head
        if prev_node is None:
            self.head = point_node
            point_node.prev = None

        else:
            prev_node.next = point_node.next
            prev_node.next.prev = prev_node

    def print_list(self):
        """Prints the list in normal list format"""
        curr_node = self.head
        print("number of applicants: [", end='')
        while curr_node is not None:
            print("{}".format(curr_node.apps), end='')
            if curr_node.next is not None:
                print(", ", end='')
            curr_node = curr_node.next
        print("]")


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
        self.apps_ll = self.init_ll(self.expected_apps)

        # attribute to hold the results of the dwgs (number of applicants selected in each point cat)
        self.dwg_results = []

    def init_ll(self, expected_apps: list):
        """Initializes a linked list to store the data associated with expected_apps"""
        linked_list = PointCatLinkList()

        for i, point_cat in enumerate(expected_apps):
            new_node = PointCatNode(i, point_cat)
            linked_list.add_node(new_node)

        return linked_list

    def run_drawing(self):
        """performs a drawing"""
        self.dwg_results = [0] * 21
        random.seed()

        num_apps = 0
        curr_node = self.apps_ll.head
        while curr_node is not None:
            num_apps += curr_node.adj_apps
            curr_node = curr_node.next

        for _ in range(self.num_tags):
            rand_num = random.randint(1, num_apps)

            # find the node that contains this number
            curr_node = self.apps_ll.head
            apps_up_to = curr_node.adj_apps
            while apps_up_to < rand_num:
                curr_node = curr_node.next
                apps_up_to += curr_node.adj_apps

            # add this node point_val to the dwg result
            curr_node.apps -= 1
            self.dwg_results[curr_node.point_val] += 1
            if curr_node.point_val == 0:
                curr_node.adj_apps -= 1
                num_apps -= 1
            else:
                curr_node.adj_apps -= (curr_node.point_val ** 2)
                num_apps -= (curr_node.point_val ** 2)

        return self.dwg_results
