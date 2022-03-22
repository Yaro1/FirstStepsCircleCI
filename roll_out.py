import sys

if __name__ == "__main__":
    n = len(sys.argv)
    print("Total arguments passed:", n)
    print("\nArguments passed:", end=" ")
    for i in range(1, n):
        print(sys.argv[i], end=" ")
    print("\n")