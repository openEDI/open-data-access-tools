import planetary_computer

def get_fs(account='nrel', container='oedi'):
    return planetary_computer.get_adlfs_filesystem(account, container)

def get_size(path, units='B'):
    fs = get_fs()
    size = fs.du(path, total=True)
    if units=='B':
        pass
    elif units=='kB':
        size = size * 10 ** -3
    elif units=='MB':
        size = size * 10 ** -6
    elif units=='GB':
        size = size * 10 ** -9
    elif units=='TB':
        size = size * 10 ** -12
    elif units=='PB':
        size = size * 10 ** -15
    elif units=='kiB':
        size = size * 2 ** -10
    elif units=='MiB':
        size = size * 2 ** -20
    elif units=='GiB':
        size = size * 2 ** -30
    elif units=='TiB':
        size = size * 2 ** -40
    elif units=='PiB':
        size = size * 2 ** -50
    else:
        raise NotImplementedError(f'Units "{units}" not recognized.')

    return size
