import zipfile
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--path", type=str, help="path to zip file")

    parser.add_argument("--outpath", type=str, help="path to zip file")

    args = parser.parse_args()
    with zipfile.ZipFile(args.path, "r") as zip_ref:
        zip_ref.extractall(args.outpath)
