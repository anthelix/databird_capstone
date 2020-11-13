from os import system, listdir
from shutil import move
from os import mkdir, getcwd, chdir


def jup2pdf():
    '''
    check ipynb files in "notebooks" and save them in project_review in html
    '''
    path = "/home/anthelix/Documents/workflow/databird/projet_review/notebooks"
    path_out = "/home/anthelix/Documents/workflow/databird/projet_review"
    #print("Please input a run_no ")
    #print("Only integers allowed")
    #run_no = int(input())
    #run_dir_name = 'html' + str(run_no)

    #mkdir(run_dir_name)

    other_stuff = []
    for f in listdir(path):
        if f.endswith("ipynb"):
            system("jupyter nbconvert --output-dir='../' --to html " + f)
            # convert to html
            f_html = '.'.join([f[: -6], 'html'])
            # make the corresponding pdf file name
            #move(f_html, run_dir_name, )
            # move the corresponding pdf file
        else:
            print('You have something other than a Jupyter notebook.')
            print(f)
            other_stuff.append(f)
    print()
    print('The following html files have been created.')
    print('in', path_out)
    
    #for f in listdir(run_dir_name):
    for f in listdir(path_out):
        if f.endswith("html"):
            print(f)
    print()
    print('We also found the following')
    print('in', path)
    for other_item in other_stuff:
        print(other_item)

    return None


'''
check files ipynb, if true execut jup2pdf function.
'''
path = "/home/anthelix/Documents/workflow/databird/projet_review/notebooks"

check_pres_of = [f.endswith("ipynb") for f in listdir("../notebooks")]

if (True in check_pres_of):    
    chdir(path)
    print('Found Jupyter notebooks in', getcwd())
    jup2pdf()
    # print([f.endswith("ipynb") for f in listdir(".")])
else:
    print('Jupyter notebooks not found in ', getcwd())