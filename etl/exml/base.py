import os

from lxml import etree


class BaseXML(object):
    """
    Base class for data extraction from XML data feeds.
    """
    def __init__(self, settings):
        self.directory = settings.XML_DATA_DIR
        self.data_file = settings.XML_FILE
        self.supplier = None
        self.feed_class = None

    def extract(self):
        filename = os.path.join(self.directory, self.data_file)
        target_parser = FeedParser(self.feed_class)
        root_elements = etree.parse(filename, etree.XMLParser(target=target_parser))
        return root_elements[0]


class FeedElement(object):
    """
    Class to apply extraction methods to a structured XML feed.  Sub-classed by SoccerFeed class.
    """
    def __init__(self, **kwargs):
        self.attributes = dict(kwargs)
        self.children = []
        self.data = ""

    def add_child(self, child):
        self.children.append(child)

    def add_data(self, data):
        text = data.encode("utf-8").strip()
        if text:
            self.data += text

    def get_children(self, cls, count=None):
        child_list = [x for x in self.children if x.__class__ == cls]
        if count is None:
            return child_list
        if len(child_list) < count:
            return None
        if count == 1:
            return getattr(child_list[0], "data", "")
        return child_list[:count]


class FeedParser(object):
    """
    Target parser class for XML document with known structure.
    """
    def __init__(self, _cls):
        self.feed_class = _cls
        self._reset()

    def _reset(self):
        self.feed_elements = []
        self.tag_stack = []
        self.roots = []

    def _find_element_class(self, tag):
        if len(self.feed_elements) > 0:
            if self.feed_elements[-1] is not None:
                return getattr(self.feed_elements[-1], tag, None)
            else:
                return getattr(self.feed_class, tag, None)
        return getattr(self.feed_class, tag, None)

    def start(self, tag, attributes):
        self.tag_stack.append(tag)
        cls = self._find_element_class(tag)
        if cls is not None:
            # print tag, cls, attributes
            self.feed_elements.append(cls(**attributes))
        else:
            self.feed_elements.append(None)

    def data(self, data):
        element = self.feed_elements[-1]
        if element is not None:
            element.add_data(data)

    def end(self, tag):
        ending_element = self.feed_elements.pop()
        _ = self.tag_stack.pop()
        if ending_element is None:  # Not a recognized element
            return
        if len(self.feed_elements) > 0:
            if self.feed_elements[-1] is not None:
                self.feed_elements[-1].add_child(ending_element)
            else:
                self.roots.append(ending_element)

    def close(self):
        roots = self.roots
        self._reset()
        return roots
