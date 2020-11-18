import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

# Independent Variables
fail_rate = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, .00001, .00001, .00001, .001, .001, .001, .01, .01, .01, .01, .01, .01, .01, .01, .01]
join_leave_rate = [.0001, .0001, .0001, .0001, .0001, .001, .001, .001, .01, .01, .0001, .0001, .0001, .0001, .0001, .0001, .0001, .0001, .0001, .001, .001, .001, .01, .01, .01]
client_rate = [3, 2, 1, .5, .1, 1, .5, .1, .5, .1, 1, .1, .05, 1, .1, .05, 1, .5, .1, 1, .5, .1, 1, .5, .1]

# Dependent Variables
insert_hops = [12, 12, 12, 12, 12, 11, 8, 10, 10, 9, 12, 4, 12, 12, 10, 12, 7, 9, 5, 8, 9, 7, 9, 7, 5]
insert_recvd = [10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 8, 10, 10, 9, 9, 10, 9, 8, 10, 10, 6, 10, 7]
get_hops = [12, 12, 12, 12, 12, 11, 9, 10, 11, 10, 12, 4, 12, 12, 10, 12, 8, 8, 5, 9, 9, 7, 9, 7, 5]
get_recvd = [10, 10, 10, 10, 10, 10, 10, 10, 10, 9, 10, 9, 8, 10, 10, 9, 5, 8, 9, 6, 10, 10, 5, 6, 7]

num_nodes = [10, 20, 30, 40, 50, 60]
stabilization_time = [6.36, 11.52, 16.33, 25.07, 38.46, 51.01]
stabilization_time_fingers = [6.946756, 14.029971, 26.244583, 37.977037, 46.021314, 0]

if __name__ == "__main__":
    '''
    # setup the figure and axes
    fig = plt.figure(figsize=(8, 3))
    ax1 = fig.add_subplot(121, projection='3d')

    # fake data
    _x = np.arange(4)
    _y = np.arange(5)
    _xx, _yy = np.meshgrid(_x, _y)
    x, y = _xx.ravel(), _yy.ravel()
    top = x + y

    x = join_leave_rate
    y = client_rate
    top = get_hops

    bottom = np.zeros_like(top)
    width = depth = 1

    ax1.bar3d(x, y, bottom, width, depth, top, shade=True)
    plt.show()
    '''

    '''
    fail_rate_disp = np.log10(.00001 + np.array(fail_rate))
    join_leave_disp = np.log10(join_leave_rate)
    get_success = np.array(get_recvd) / 10

    plt.scatter(client_rate[10:], get_success[10:])
    plt.title("")
    plt.xlabel("Log Node Failure Rate")
    plt.ylabel("Avg Hops/Req")
    plt.show()
    '''


    plt.scatter(num_nodes, stabilization_time_fingers)
    plt.title("Stabilization Time (w/ Finger Tables)")
    plt.xlabel("Number of Nodes")
    plt.ylabel("Time to Stabilize")
    plt.show()

