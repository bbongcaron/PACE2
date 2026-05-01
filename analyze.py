#!/usr/bin/env python3
# potcheck.py

"""Read a given potential table file, check for 'compliance', print the results."""

import sys
import numpy as np
import matplotlib.pyplot as plt

input_file = sys.argv[1]

def main(potential_table) -> int:
    checks=[0 for i in range(3)]
    with open(potential_table) as f:
        table_data = np.loadtxt(f, usecols=(0,1), comments=("#","@"))
        print(table_data)
        plt.plot(table_data[:,0], table_data[:,1])
        plt.axhline(y=0, color='r', linestyle='--') 
        plt.ylabel(r"$U(r)$ (kJ/mol)")
        plt.xlabel(r"$r$ (nm)")
        plt.show()
        if table_data[0,1] > 10:         checks[0]+=1
        if table_data[:,1].min() < 0:   checks[1]+=1
        if table_data[0,1] > 100:       checks[2]+=1
        
    print(' '.join(list(str(i) for i in checks)))
    return checks

if __name__ == '__main__':
    main(input_file)