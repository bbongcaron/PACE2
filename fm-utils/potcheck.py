#!/usr/bin/env python3
# potcheck.py

"""Read a given potential table file, check for 'compliance', print the results."""

import sys, os, json
import numpy as np
import matplotlib.pyplot as plt

def updateParameterJSON(result: str):
    hyperparameters_fname = "hyperparameters.json"
    with open(hyperparameters_fname, "r") as file:
        data = json.load(file)
    
    data['result'] = result
    with open("data.json", "w") as file:
        json.dump(data, file, indent=4)
    
def main():
    checks=[0 for i in range(3)]
    pot_file = sys.argv[1]
    

    if not os.path.exists(pot_file):
        print(' '.join(list(str(i) for i in checks)) + "no pot")
        return checks

    with open(pot_file) as f:
        table_data = np.loadtxt(f, usecols=(0,1), comments=("#","@"))
        plt.plot(table_data[:,0], table_data[:,1])
        plt.axhline(y=0, color='r', linestyle='--') 
        plt.ylabel(r"$U(r)$ (kJ/mol)")
        plt.xlabel(r"$r$ (nm)")
        if table_data[0,1] > 10:         checks[0]+=1
        if table_data[:,1].min() < 0:   checks[1]+=1
        if table_data[0,1] > 100:       checks[2]+=1
    
    result = ' '.join(list(str(i) for i in checks))
    print(result)
    plt.savefig(f"{result}.png")
    plt.close()
    #updateParameterJSON(result)
    return checks

if __name__ == "__main__":
    main()