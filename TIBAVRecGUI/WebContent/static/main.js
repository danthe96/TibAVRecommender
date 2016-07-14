var iframes = document.getElementsByTagName('iframe');

iframes[0].onload = function() {
	document.title = iframes[0].contentWindow.document.getElementById(
			'videoTitle').concat(" | Knowledge Recommender");
}