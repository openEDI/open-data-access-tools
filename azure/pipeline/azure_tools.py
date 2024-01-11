import planetary_computer

def get_fs(account='nrel', container='oedi'):
    return planetary_computer.get_adlfs_filesystem(account, container)

def get_size(path, units='B'):
    fs = get_fs()
    size = fs.du(path, total=True)
    match units:
        case 'B':
            pass
        case 'kB':
            size = size * 10 ** -3
        case 'MB':
            size = size * 10 ** -6
        case 'GB':
            size = size * 10 ** -9
        case 'TB':
            size = size * 10 ** -12
        case 'PB':
            size = size * 10 ** -15
        case 'kiB':
            size = size * 2 ** -10
        case 'MiB':
            size = size * 2 ** -20
        case 'GiB':
            size = size * 2 ** -30
        case 'TiB':
            size = size * 2 ** -40
        case 'PiB':
            size = size * 2 ** -50
        case _:
            raise NotImplementedError(f'Units "{units}" not recognized.')

    return size
