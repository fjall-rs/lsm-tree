import itertools
import subprocess

features_list = [
    "cascading",
    # "fast_partition_point",
    "use_unsafe"
]

def run_with_features(features):
    """
    Constructs the cargo command and runs it with the specified features.
    Prints the command being run and its output.
    """
    if not features:
        # Handle the case of no features (though technically a combination of size 0)
        # If you want to run with no features, you might adjust this
        features_arg = ""
        command = ["cargo", "run", "-r"]
        # print("--- Running command: cargo run -r (no features) ---")
    else:
        features_arg = ",".join(features)
        command = ["cargo", "run", "-r", "--features", features_arg]
        # print(f"--- Running command: {' '.join(command)} ---")

    try:
        # Run the command and capture output
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,  # Capture output as text
            check=True  # Raise an exception if the command fails
        )
        # print("--- Output ---")
        print(result.stdout.strip())
        # if result.stderr:
        #     print("--- Stderr ---")
        #     print(result.stderr)

    except subprocess.CalledProcessError as e:
        print(f"--- Command failed with error: {e} ---")
        print(f"--- Stderr ---")
        print(e.stderr)
    except FileNotFoundError:
        print("--- Error: 'cargo' command not found. Is Cargo installed and in your PATH? ---")
    except Exception as e:
        print(f"--- An unexpected error occurred: {e} ---")

if __name__ == "__main__":
    # Generate combinations of different lengths (from 1 to the total number of features)
    all_combinations = []

    for i in range(1, len(features_list) + 1):
        combinations_of_length_i = itertools.combinations(features_list, i)
        all_combinations.extend(list(combinations_of_length_i))

    all_combinations.append(tuple())
    all_combinations.sort(key=len)

    # Include the case with no features (an empty combination) if desired
    # all_combinations.append(tuple()) # Uncomment this line to include running with no features

    # Loop over each combination
    for combination in all_combinations:
        # Run the cargo command with the current combination's features
        # Convert the tuple to a list for the join operation
        run_with_features(list(combination))
        # print("\n" + "="*50 + "\n") # Separator for clarity

