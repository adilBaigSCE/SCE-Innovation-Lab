import matplotlib.pyplot as plt
import numpy as np

N = int(input('Enter the number of axis: '))

# Create axis
axes = [N, N, N]

# Generate Data for the cube
data = np.ones(axes, dtype=np.bool_)

#print(data)
# Control Transparency
alpha = 0.0

# Control colour
colors = np.empty(axes +[4], dtype=np.float32)

i = 0
while i < N:
    colors[i] = [0, 0, 0, alpha]
    i += 1

# Plot figure
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')

# Voxels are used to customizations of the sizes, positions and colors.
ax.voxels(data, facecolors=colors, edgecolors='black')

ax.set_title(f"Here's a {N} X {N} X {N} Cube with {N**3} Cubes Inside")

plt.show()
