#!/bin/bash

# Define the local files you want to add
FILE1_PATH="../run-inference-pipeline.yml"
FILE2_PATH="../run-inference-instance.yml"

while IFS= read -r line || [[ -n "$line" ]]; do
	repo=$(echo "$line" | sed 's/[^a-zA-Z0-9/-]//g')
    echo "Processing $repo..."

    # Clone the repository
    gh repo fork ersilia-os/$repo --clone=true 
    cd $repo

    # Create a new branch (for example: "add-local-files")
    git checkout -b add-inference-files

    # Copy the local files to this repo
    cp "$FILE1_PATH" ./.workflows/
    cp "$FILE2_PATH" ./.workflows/

    # Commit the added files
    git add .
    git commit -m "Added inference files"

    # Push the branch to GitHub
    git push -u origin add-inference-files

    # Create a pull request
    gh pr create --base main --head dmc-au:add-inference-files --repo ersilia-os/$repo --title "Adding inference files." --body "Adding the inference pipeline files."

    # Return to the original directory and remove the temporary directory
    cd ..
    rm -rf $repo
done < $1
