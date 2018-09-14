#!/usr/bin/python3
import pandas as pd
import matplotlib.pyplot as plt


def question_1():
    df1 = pd.read_csv('Olympics_dataset1.csv', index_col=0,
                      skiprows=1, thousands=',')
    df2 = pd.read_csv('Olympics_dataset2.csv', index_col=0,
                      skiprows=1, thousands=',')
    merged_df = pd.merge(df1, df2, left_index=True, right_index=True)
    print(merged_df.head(5))
    merged_df = merged_df[:-1]
    return merged_df


def question_2(merged_df):
    merged_df.index.name = 'country name'
    print(merged_df.head(1))


def question_3(merged_df):
    merged_df.drop(columns='Rubish', inplace=True)
    print(merged_df.head(5))


def question_4(merged_df):
    merged_df.dropna(inplace=True)
    print(merged_df.tail(10))


def question_5(merged_df):
    print(merged_df['Gold_x'].idxmax())


def question_6(merged_df):
    print((merged_df['Gold_x'] - merged_df['Gold_y']).abs().idxmax())


def question_7(merged_df):
    merged_df = merged_df.sort_values(by='Total.1', ascending=False)
    print(merged_df.head(5))
    print(merged_df.tail(5))
    return merged_df


def question_8(merged_df):
    merged_df.head(10)[['Total_x', 'Total_y']].rename(
        columns={'Total_x': 'Summer Games', 'Total_y': 'Winter Games'}).plot.barh(
        title='Medals for Winter and Summer Games', stacked=True)
    plt.show()


def question_9(merged_df):
    merged_df.ix[[
        x for x in merged_df.index if
        ('United States' in x) |
        ('Australia' in x) |
        ('Great Britain' in x) |
        ('Japan' in x) |
        ('New Zealand' in x)
    ]][['Gold_y', 'Silver_y', 'Bronze_y']].rename(
        columns={'Gold_y': 'Gold', 'Silver_y': 'Silver', 'Bronze_y': 'Bronze'}).plot.bar(
        title='Winter Games'
    )
    plt.show()


def main():
    merged_df = question_1()
    question_2(merged_df)
    question_3(merged_df)
    question_4(merged_df)
    question_5(merged_df)
    question_6(merged_df)
    merged_df = question_7(merged_df)
    question_8(merged_df)
    question_9(merged_df)


if __name__ == '__main__':
    main()
