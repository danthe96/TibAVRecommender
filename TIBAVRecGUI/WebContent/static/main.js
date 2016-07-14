var iframes = document.getElementsByTagName('iframe');


iframes[0].onload = function() {
    document.title = document.getElementsByTagName('videoTitle')[0].concat(" | Knowledge Recommender");
}