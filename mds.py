import matplotlib.pyplot as plt
counts = [[('p', 100),('n',40)], [('p', 80), ('n', 20)]]
for count in counts:
    if count:
        pos_count.append(count[0][1])
        neg_count.append(count[1][1])
ax = plt.subplot(111)
x = range(0, len(pos_count))
# pdb.set_trace()
ax.plot(x, pos_count, 'bo-', label="positive")
ax.plot(x, neg_count, 'go-', label="negative")
y_max = max(max(pos_count), max(neg_count)) + 50
ax.set_ylim([0, y_max])
plt.xlabel("Time step")
plt.ylabel("Word count")
plt.legend(fontsize='small', loc=0)
plt.savefig("plot.png")
plt.show()
