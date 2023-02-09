import matplotlib.pyplot as plt 



plt.plot(
    [1, 2, 3, 4, 5, 6 ,7], 
    [
        157, 153, 149, 147, 139, 128, 125
    ]
)

plt.plot(
    [1, 2, 3, 4, 5, 6 ,7], 
    [
        184, 137, 102, 132, 145, 159, 167
    ]
)

plt.plot(
    [1, 2, 3, 4, 5, 6 ,7], 
    [
        333, 333, 333, 333, 333, 333, 333
    ], 
    '--'
)
plt.xticks([0, 1, 2, 3, 4, 5, 6, 7], ["0", "1", "5", "10", "20", "50", "100", "250"])
plt.legend(["Blob based pipeline", "Queue based pipeline", "Locally deployed, Queue"])
plt.xlabel("Number of batches")
plt.grid()
plt.ylabel("Execution time in seconds")
plt.show()