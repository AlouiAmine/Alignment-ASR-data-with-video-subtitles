# Alignment ASR data with video subtitles:

![model diagram image](/img/strategy.jpg)

1- pick one content from the subtitle data where the start time equals to T

2- select from the ASR data the content to be compared where their start time are in the interval [t-2min,t+2min] (most of the delay between the ASR data and the subtitle is not very big  )

3- for each pair, we calculate the matching score (to calculate the matching score i used FuzzyWuzzy Python library which is used for string matching)

4- keep the pairs with greatest score

# Preprocessing the two files :

`python3 preprocess_data.py --pa_subtitles 'pa_subtitles.csv' --INA_subtitles 'INA_subtitles.json'`

# Alignment :

`python3 alignement_task.py `

`python3 alignement_task_13h_delay.py `

