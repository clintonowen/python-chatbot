# python-chatbot
> A chatbot built with Python/TensorFlow and trained by Reddit

## Highlights
* Built in a week as a side project to teach myself Python
* Uses a data dump of [1.7 billion Reddit comments](https://www.reddit.com/r/datasets/comments/3bxlg7/i_have_every_publicly_available_reddit_comment/?st=j9udbxta&sh=69e4fee7) for training
* Uses a Sequence to Sequence model in TensorFlow

<!-- ![](images/screenshot-5.png) -->

## Description
I needed a good dataset of conversations to train the chatbot with. For this, I decided to use a data dump of [1.7 billion Reddit comments](https://www.reddit.com/r/datasets/comments/3bxlg7/i_have_every_publicly_available_reddit_comment/) rather than the more commonly-used [Cornell Movie-Dialogs Corpus](https://www.cs.cornell.edu/~cristian/Cornell_Movie-Dialogs_Corpus.html) which has only ~220K lines of dialog. To get a proof-of-concept completed in a week, I worked with just [the latest month](http://files.pushshift.io/reddit/comments/) of Reddit comments (Sept. 2018), which still came out to 100M comments and over 100GB of uncompressed data. With more time, the chatbot could be trained with a larger corpus of comments to improve its functionality.

## Usage
1. Clone the repository
2. Download the desired datasets [here](https://www.reddit.com/r/datasets/comments/3bxlg7/i_have_every_publicly_available_reddit_comment/) or [here](http://files.pushshift.io) (has more current dumps) and organize your data as follows:
```
python-chatbot/
├── chatdata/
│   ├── 2017
│   └── 2018
│       └── RC_2019-09
├── db/
├── utils/
├── chatbot_database.py
├── create_training_data.py
└── README.md
```
3. Inside `chatbot_database.py`, update the `timeframe` to the year and month of the corresponding dataset you'd like to use.
4. In the terminal/shell, navigate to the project folder and run the following:
```sh
py chatbot_database.py
```
* Note: For `RC_2019-09`, the above script took 4.3 hours to complete on my mid-2015 Macbook Pro (2.8Ghz i7, 16GB RAM)

## Technology Stack
* Python3
* TensorFlow
* SQLite3
* pandas

## Meta

by Clinton Owen – [@CoderClint](https://twitter.com/CoderClint) │ clint@clintonowen.com │ [https://github.com/clintonowen](https://github.com/clintonowen)