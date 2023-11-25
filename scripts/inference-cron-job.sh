#!/bin/bash

# The purpose of this script is to run the inference pipeline for all listed models on a schedule.
# The schedule is suggested for every 7 hours, because standard runner GH jobs don't last longer than 6 hours.
# The script relies on having the AirTable (https://airtable.com/appgxpCzCDNyGjWc8/shrUcrUnd7jB9ChZV/tblZGe2a2XeBxrEHP) up to date to pull model ids that have a docker image.

# 1. Download this script file to your local machine.
# 2. Make the script executable "chmod +x trigger_workflow.sh".
# 3. Make sure the GitHub CLI (gh) is installed and authenticated on the machine where the script will run.
# 4. Replace the variable details below with your own.
# 5. Run crontab -e to edit the cron jobs for your user.
# 6. Add the following line without quotes to the crontab file to run the script every 7 hours, replacing the path:
# "0 */7 * * * /path/to/this-file.sh"
# 7. Save the crontab file and exit the editor. The cron job is now scheduled.
# 8. As long as the machine is running, the cron job will run the script every 7 hours.
# 9. To remove the cron job, run crontab -e again and delete the line you added.
# 10. If you want to run the cron job at the top of the list another time, remove the line-number.txt file and the airtable-model-list.csv.

# Variables
OWNER="ersilia-os"
REPO="model-inference-pipeline"
WORKFLOW_ID="run-precalc-pipeline.yml"
AIRTABLE_MODEL_LIST="
https://airtable-csv-exports-production.s3.amazonaws.com/d5b47025317018b4/Table.csv?AWSAccessKeyId=ASIAR7KB7OYSENZ5C4G3&Expires=1700774352&Signature=K6NUGaLap7SUFB6ancCHajZPWIQ%3D&x-amz-security-token=IQoJb3JpZ2luX2VjEPb%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJGMEQCIFl5Oxf%2BEyoN4%2FnBypZ5ZMkP7HBszdN7KLAc55EoiaFYAiAaDi%2FcLc4uhQGkZdRF%2FrNKVdJosnwJdclbWOcI6kOOISqzAghOEAAaDDEzNTk2NjcxNzQ3NiIM4ws581%2FsJdioGBSjKpACmzd5SN8F1pw4vG7FY%2BkYZAnSpeN6rXf7D5qw2qO48cj%2FLNNafuZ7UAtJMR75hHDD97fTEBXhywlw9ktWXenVK6xSh6KV6bGE9v9HdLBd4DukqIO8sqJCvX7DPSKANecSIdw6TUTNw77Ens%2F0s3gaSS9FtfW%2FOY3BnwzsTs5WUbkL%2FwdFAzaG2pb6Tt%2F%2FVfmf%2BSPvDkJkF1YrdE2vy7zH4QeYP9Dxa0AXbuIMhP3N4G1BpIwfhqsRoulQdcTdvoajHeFCMrkZEK1l7MYzYlgHaYckDYC0tmmUTehCDPRYQACGDPxzVNsgxsCduiCE9yVPwAqKmGtsqtJe12hmB2yKmoojoZrILZ1UKTDii%2BXy8Q4wpID%2FqgY6ngGaFo3QkIrzXQufNfVcjCet8dz9TN65qYRHYRqEpVXDSHGBxH2xMFbshEHeNeNQAqJsyyLUftXqie8VWgRug4RspjXHUeXHIMUGNQRPmXTKCslyQTV0qs%2BRdiJPJuEmnr3O%2F%2FxpN5Xcv2T0X8gQnXbh91wSgAGcjXwc9E%2FxUWOiyGxfrel4DFBMOKc5Qlg3SbwK%2FcXYO2Z%2F34IDSOAFFw%3D%3D"  # Replace with your actual CSV URL
CSV_MODEL_LIST="airtable-model-list.csv"
BRANCH="main"  # or any branch you want to run the workflow on
LINE_NUMBER_FILE="./line-number.txt"

# Download the Airtable CSV file if it doesn't already exist
# if [ ! -f "$CSV_MODEL_LIST" ]; then
#     echo "Downloading CSV file..."
#     curl -o "$CSV_MODEL_LIST" "$AIRTABLE_MODEL_LIST"
# fi

# Ensure the line number file exists
touch $LINE_NUMBER_FILE

# Read the current line number
LINE_NUMBER=$(cat $LINE_NUMBER_FILE)

# Assuming column 1 is Identifier and column 2 is DockerHub
INPUT=$(awk -v line=$LINE_NUMBER -F, 'NR == line {if ($2 != "") print $1}' $CSV_FILE)

# Increase the line number for the next run and save it
((LINE_NUMBER++))
echo $LINE_NUMBER > $LINE_NUMBER_FILE

# Check if INPUT is not empty
if [ ! -z "$INPUT" ]; then
  # Trigger the workflow using GitHub CLI
  gh api /repos/$OWNER/$REPO/actions/workflows/$WORKFLOW_ID/dispatches \
   --input - <<< '{"ref":"'$BRANCH'","inputs":{"model-id":"'$INPUT'"}}'

  # TODO: Add condition to exit script, not sure if succesful run return sends anything.
else
  echo "No valid DockerHub entry found on line $LINE_NUMBER"
fi