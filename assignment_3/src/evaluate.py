import pandas as pd
import os
from sklearn.metrics import r2_score

def compare_averages(computed_file, extracted_file):
    computed_df = pd.read_csv(computed_file)
    extracted_df = pd.read_csv(extracted_file)
    
    
    