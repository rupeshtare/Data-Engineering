# basic_commands_lab.sh
# Beginner Level: The very first steps in Linux

echo "--- 1. NAVIGATION ---"
pwd      # Where am I?
ls       # What is in this folder?
mkdir -p my_first_folder
cd my_first_folder
pwd

echo "--- 2. FILE CREATION ---"
echo "Hello Data Engineer" > learning.txt
cat learning.txt

echo "--- 3. INTERMEDIATE: PERMISSIONS ---"
ls -l learning.txt
chmod 400 learning.txt # Make it Read-Only
ls -l learning.txt

echo "--- 4. ARCHITECT: LOG INSPECTION ---"
# Imagine learning.txt is a huge log file
echo "ERROR: Data shuffle failed" >> learning.txt
grep "ERROR" learning.txt

# 🏛️ Architect's Tip:
# "In production, you'll rarely have a GUI. Get comfortable with 
# 'grep' and 'tail -f'. Being able to find an error in a 10GB log file 
# is what separates a junior from an architect."
