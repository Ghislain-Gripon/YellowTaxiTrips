import logging, pathlib
from FolderStructure import FolderStructure
from Workflow import Workflow

def main():
    
    logging.basicConfig(format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s', level=logging.INFO)
    logging.info("Launching configuration procedures.")
    FileHandler = FolderStructure(pathlib.Path.cwd())
    Workflow(FileHandler)
    

if __name__ == "__main__":
# execute only if run as a script
    main()




