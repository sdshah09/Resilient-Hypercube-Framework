import matplotlib.pyplot as plt
import pandas as pd

# Benchmark data
data = {
    "peer": ["5555", "5555", "5555", "5555", "5556", "5556", "5556", "5556",
             "5557", "5557", "5557", "5557", "5558", "5558", "5558", "5558",
             "5559", "5559", "5559", "5559", "5560", "5560", "5560", "5560",
             "5561", "5561", "5561", "5561", "5562", "5562", "5562", "5562"],
    "api": ["create_topic", "publish", "subscribe", "pull"] * 8,
    "latency": [0.0044, 0.0343, 0.0047, 0.0046, 0.0066, 0.0381, 0.0082, 0.0069,
                0.0019, 0.0300, 0.0023, 0.0021, 0.0061, 0.0322, 0.0064, 0.0049,
                0.0090, 0.0394, 0.0083, 0.0082, 0.0152, 0.0461, 0.0167, 0.0151,
                0.0075, 0.0428, 0.0099, 0.0071, 0.0098, 0.0469, 0.0116, 0.0098],
    "throughput": [228.47, 29.16, 214.43, 215.24, 151.66, 26.28, 122.54, 144.26,
                   525.09, 33.34, 431.99, 467.74, 164.15, 31.09, 156.28, 202.22,
                   110.95, 25.40, 120.65, 121.61, 65.98, 21.70, 60.05, 66.14,
                   133.20, 23.35, 101.13, 141.13, 102.37, 21.32, 86.13, 101.68]
}

# Convert data to a DataFrame for easier manipulation
df = pd.DataFrame(data)

# Plot latency for each API across peers
def plot_latency(df):
    plt.figure(figsize=(12, 8))
    for api in df['api'].unique():
        api_data = df[df['api'] == api]
        plt.bar(api_data['peer'] + ' ' + api, api_data['latency'], label=api)

    plt.xlabel("Peers and API")
    plt.ylabel("Latency (seconds)")
    plt.title("API Latency across Peers")
    plt.legend(title="API")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()

# Plot throughput for each API across peers
def plot_throughput(df):
    plt.figure(figsize=(12, 8))
    for api in df['api'].unique():
        api_data = df[df['api'] == api]
        plt.bar(api_data['peer'] + ' ' + api, api_data['throughput'], label=api)

    plt.xlabel("Peers and API")
    plt.ylabel("Throughput (requests per second)")
    plt.title("API Throughput across Peers")
    plt.legend(title="API")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()

# Generate the plots
plot_latency(df)
plot_throughput(df)
