import matplotlib.pyplot as plt

x = [2, 3, 4]
y = [1, 1.29, 1.65]

plt.plot(x, y, color='blue', marker='o')
plt.xticks(x)

plt.xlabel('Numero di Worker')
plt.ylabel('Speedup')
plt.title('Speedup per Worker')
plt.grid(True, linestyle='--', alpha=0.6)

plt.show()