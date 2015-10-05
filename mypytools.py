# mypytools.py
# - Raoul L. Veroy

def merge_two_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z

if __name__ == "__main__":
    pass

__all__ = [ "merge_two_dicts", ]
