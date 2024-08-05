from datetime import datetime

# Test Inputs
manual = [
    [datetime(1996,3,11), datetime(1996,4,11)],
    [datetime(1996,3,11), datetime(1996,4,11)],
    [],
    [datetime(1996,3,11), datetime(1996,4,11)],
]

estimated = [
    [datetime(1996,3,8), datetime(1996,5,11)],
    [datetime(1996,3,8), datetime(1996,3,2)],
    [datetime(1996,3,8), datetime(1996,3,2)],
    []
]

# Test expected answers
matched = [
    [[3, datetime(1996, 3, 11, 0, 0), datetime(1996, 3, 8, 0, 0)]],
    [[3, datetime(1996, 3, 11, 0, 0), datetime(1996, 3, 8, 0, 0)]],
    [],
    []
]

only_manual = [
    [datetime(1996, 4, 11, 0, 0)],
    [datetime(1996, 4, 11, 0, 0)],
    [],
    [datetime(1996, 3, 11, 0, 0), datetime(1996, 4, 11, 0, 0)],
]

only_estimated = [
    [datetime(1996, 5, 11, 0, 0)],
    [datetime(1996, 3, 2, 0, 0)],
    [datetime(1996, 3, 8, 0, 0), datetime(1996, 3, 2, 0, 0)],
    []
]

answer = [
    matched,
    only_manual,
    only_estimated,
]