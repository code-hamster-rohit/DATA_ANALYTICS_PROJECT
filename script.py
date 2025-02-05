from wordcloud import WordCloud
import matplotlib.pyplot as plt
import nltk, subprocess, re, os
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')

def load_data(file_path):
    with open(file_path, 'r') as f:
        data = f.read()
    return data

def clean_data(text):
    lemmatizer = WordNetLemmatizer()
    cleaned_text = re.sub(r'\b\w*\d\w*\b', '', text)
    cleaned_text = re.sub(r'[^A-Za-z\s]', '', text)
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
    cleaned_text = cleaned_text.lower()
    words = word_tokenize(cleaned_text)
    stop_words = set(stopwords.words("english"))
    lemmetized_words = [lemmatizer.lemmatize(word) for word in words if word not in stop_words]
    cleaned_text = " ".join(lemmetized_words)
    return cleaned_text.strip()

def run_powershell_commands(command):
    try:
        process = subprocess.Popen(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        while True:
            output = process.stdout.readline()
            if output:
                print(output.strip())
            if process.poll() is not None:
                break

        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            print("\u2705 Commands executed successfully.")
            if stdout:
                print(stdout)
        else:
            print("\u274C Error occurred during execution:")
            if stderr:
                print(stderr)
    except Exception as e:
        print(f"\u26A0\ufe0f Exception occurred: {e}")

def generate_word_cloud(data, output_file="wordcloud.png"):
    processed_data = {}
    for line in data.strip().split("\n"):
        if "\t" in line:
            word, count = line.split("\t")
            processed_data[word.strip('"')] = int(count)

    wordcloud = WordCloud(
        width=1000,
        height=800,
        background_color="white",
        colormap="viridis"
    ).generate_from_frequencies(processed_data)

    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.title("Word Cloud")
    plt.show()

    wordcloud.to_file(output_file)
    print(f"Word cloud saved to {output_file}")

if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))

    command = f'''
    cd {path};
    docker-compose up -d;
    sleep 10;
    docker exec -it namenode bash -c "hdfs dfsadmin -safemode leave; hdfs dfs -rm -r -f /user/root; hdfs dfs -mkdir -p /user/root; exit";
    docker cp hadoop-mapreduce-examples-2.7.1-sources.jar namenode:/tmp;
    docker cp data.txt namenode:/tmp;
    docker exec -it namenode bash -c "cd /tmp; hdfs dfsadmin -safemode leave; hdfs dfs -mkdir -p /user/root/input; hdfs dfs -put -f /tmp/data.txt /user/root/input; exit";
    docker exec -it namenode bash -c "hdfs dfsadmin -safemode leave; hadoop jar /tmp/hadoop-mapreduce-examples-2.7.1-sources.jar org.apache.hadoop.examples.WordCount /user/root/input /user/root/output; hdfs dfs -cat /user/root/output/part-r-00000 > /tmp/output.txt; exit";
    docker cp namenode:/tmp/output.txt output.txt;
    docker-compose down;
    '''

    with open('data.txt', 'r') as f:
        raw_data = f.read()

    data = clean_data(raw_data)

    with open('data.txt', 'w') as f:
        f.write(data)

    run_powershell_commands(command)

    with open('output.txt', 'r') as f:
        data = f.read()

    generate_word_cloud(data)