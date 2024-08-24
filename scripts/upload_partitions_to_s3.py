import os
import subprocess

numerator = int(os.environ.get("numerator"))
input_sha = os.environ.get("SHA")

output_file = "../{}_{}.csv".format(input_sha, numerator)

s3_destination = "s3://precalculations-bucket/out/{}/{}/{}_{}.csv".format(
    os.environ.get("model-id"), input_sha, input_sha, numerator
)
subprocess.run(["aws", "s3", "cp", output_file, s3_destination])
