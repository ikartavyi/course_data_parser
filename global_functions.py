# Here we store some simple global functions we could use everywhere
def list_safe_get(list_obj, index):
    """
    Return object from list on specified index. If there is no such object returns None
    :param list_obj:
    :param index:
    :return: list_object on specified index or None
    """
    result = None

    if len(list_obj) - 1 >= index:
        result = list_obj[index]

    return result
