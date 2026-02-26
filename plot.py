import pandas as pd
import matplotlib.pyplot as plt

def plot_performance():
  df = pd.read_csv('benchmark_results_8-128.csv')

  # Pivot the data to get the correct structure for plotting
  # Index (X-axis) -> Workers
  # Columns (Legend/Bars) -> Partitions
  # Values (Y-axis) -> Duration_Seconds
  pivot_df = df.pivot(index='Workers', columns='Partitions', values='Duration_Seconds')

  ax = pivot_df.plot(kind='bar', figsize=(12, 7), width=0.8, edgecolor='black', zorder=3)

  plt.title('Execution Time by Workers and Partitions', fontsize=16, pad=20)
  plt.xlabel('Number of Workers', fontsize=12)
  plt.ylabel('Duration (Seconds)', fontsize=12)
  plt.xticks(rotation=0)  # Keep worker numbers horizontal
  
  # Legend setup
  plt.legend(title='Partitions', title_fontsize='11', loc='upper right')
  
  # Add gridlines behind the bars
  plt.grid(axis='y', linestyle='--', alpha=0.7, zorder=0)

  # 6. Add value labels on top of each bar for clarity
  for p in ax.patches:
    # p.get_height() is the value (seconds)
    if p.get_height() > 0: # Only label non-zero bars
      ax.annotate(f'{p.get_height():.0f}', 
                  (p.get_x() + p.get_width() / 2., p.get_height()), 
                  ha='center', va='bottom', 
                  fontsize=9, xytext=(0, 5), 
                  textcoords='offset points')

  # 7. Save and Show
  plt.tight_layout()
  plt.savefig('performance_chart.png') # Saves the image
  print("Plot saved as 'performance_chart.png'")
  plt.show()

if __name__ == "__main__":
  plot_performance()