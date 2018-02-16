In order to launch an evaluation you need to install the python module
(requests)[http://docs.python-requests.org/en/master/user/install/#install]

```
$ sudo pip install requests
```

# Plot results

Install matplotlib and pandas and run the scripts:

```
$ sudo pip install matplotlib
$ sudo pip install pandas
# parse data from experiment output and persists the result to json files
$ python parse_results.py <folder_name>
# load the parsed results and generate plots
$ python plot_results.py <folder_name>
```
