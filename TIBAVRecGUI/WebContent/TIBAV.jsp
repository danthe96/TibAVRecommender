<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8" import="java.sql.*,org.jsoup.Jsoup" %>

<%
	int[] recId = { 18564, 19017, 15907 };
	int video_id = 16350;
	String logs = "";
	String title = "(Knowledge)Recommender";
	try {
		video_id = Integer.parseInt(request.getPathInfo().replace("/", ""));

		Class.forName("com.mysql.jdbc.Driver");
		Connection db_con = DriverManager.getConnection("jdbc:mysql://172.16.65.75:3307/tibav?user=root&password=knowmintibav");
		try {
			logs += "Video ID is " + video_id + "\n";
			PreparedStatement getRecommendations = db_con
					.prepareStatement("SELECT VIDEO_A, VIDEO_B, score, keywords FROM rec1407 WHERE VIDEO_A=? ORDER BY score DESC LIMIT 3");
			getRecommendations.setInt(1, video_id);
			ResultSet recommendationResult = getRecommendations.executeQuery();
			if (!recommendationResult.first()) {
				logs += "No results in database\n";
				//response.sendError(HttpServletResponse.SC_NOT_FOUND);
				//return;
			} else {
				for (int i = 0; i < 3 && !recommendationResult.isAfterLast(); i++) {
					recId[i] = recommendationResult.getInt(2);
					recommendationResult.next();
				}
			}
			recommendationResult.close();
			getRecommendations.close();

		} catch (SQLException e) {
			//response.sendError(HttpServletResponse.SC_NOT_FOUND);
			//return;
			logs += "\ncaught first sql exception: ";
			logs += e.getMessage();
		}
		try {
			PreparedStatement getTitle = db_con.prepareStatement("SELECT title FROM tibav.tibvid WHERE videoid=?");
			getTitle.setInt(1, video_id);
			ResultSet titleResult = getTitle.executeQuery();
			titleResult.next();
			title = titleResult.getString("title") + " | KnowledgeRecommender";
			titleResult.close();
			getTitle.close();
		} catch (SQLException e) {
			//response.sendError(HttpServletResponse.SC_NOT_FOUND);
			//return;
			logs += "\ncaught second sql exception: ";
			logs += e.getMessage();
		}
		
		db_con.close();
		
	} catch (NumberFormatException e) {
		response.sendError(HttpServletResponse.SC_NOT_FOUND);
		return;
	}
%>

<!DOCTYPE html>
<html
	class="js flexbox flexboxlegacy canvas canvastext webgl touch geolocation postmessage no-websqldatabase indexeddb hashchange history draganddrop websockets rgba hsla multiplebgs backgroundsize borderimage borderradius boxshadow textshadow opacity cssanimations csscolumns cssgradients no-cssreflections csstransforms csstransforms3d csstransitions fontface generatedcontent video audio localstorage sessionstorage webworkers applicationcache svg inlinesvg smil svgclippaths"
	style="" xmlns="http://www.w3.org/1999/xhtml" lang="en">
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8">
<meta charset="UTF-8">
<title><%=title%></title>
<meta name="viewport"
	content="width=device-width, initial-scale=1, user-scalable=yes">
<link rel="stylesheet" type="text/css" href="static/less.css">
<style type="text/css">
:root #content>#right>.dose>.dosesingle, :root #content>#center>.dose>.dosesingle
	{
	display: none !important;
}
</style>
</head>
<body>
	<div id="wrapper">
		<header class="header-up">
			<div class="wrap">
				<span id="nav-toggle"></span> <a id="logo" href="https://av.tib.eu/"
					title="Home"> <img src="static/TIB_Logo_AV-Portal.png"
					alt="TIB-AV" height="105" width="357">
				</a>


				<div class="clear"></div>
			</div>
		</header>
		<div id="content">
			<div class="wrap">
				<div id="id70" style="display: none"></div>
				<div vocab="http://schema.org/" typeof="Movie">
					<iframe height="600" scrolling="no" style="padding-bottom: 24px;"
						src="<%="http://av.tib.eu/player/" + video_id%>" frameborder="0"
						allowfullscreen></iframe>
				</div>

				<div class="detail-head">
					<h3>Recommendations</h3>
				</div>

				<div id="searchresult" class="more-like-this">
					<div>
						<div class="searchresult-item" vocab="http://schema.org/"
							typeof="Movie">

							<iframe height="315" scrolling="no"
								src="http://av.tib.eu/player/<%=+recId[0]%>" frameborder="0"
								allowfullscreen></iframe>

							<div class="searchresult-title">
								<a href="<%=recId[0]%>" class="resultTitle" rel=""
									property="name"
									title="2/4 Singular support of coherent sheaves" lang="en">Recommendation
									1</a>
							</div>

							<div class="searchresult-subline">
								<span class="i-time duration"></span> <span class="publisher">
									<span property="publisher"
									title="Institut des Hautes Études Scientifiques (IHÉS)">Institut
										des Hautes Études Scientifiques (IHÉS)</span>
								</span> <span class="language"> <span title="English">English</span>
								</span> <span class="releaseyear"> <span title="2015">2015</span>
								</span>
								<div class="clear"></div>
							</div>
						</div>

						<div class="searchresult-item" vocab="http://schema.org/"
							typeof="Movie">

							<iframe height="315" scrolling="no"
								src="http://av.tib.eu/player/<%=+recId[1]%>" frameborder="0"
								allowfullscreen></iframe>

							<div class="searchresult-title">
								<a href="<%=recId[1]%>" class="resultTitle" rel=""
									property="name"
									title="2/4 Singular support of coherent sheaves" lang="en">Recommendation
									2</a>
							</div>

							<div class="searchresult-subline">
								<span class="i-time duration"></span> <span class="publisher">
									<span property="publisher"
									title="Institut des Hautes Études Scientifiques (IHÉS)">Institut
										des Hautes Études Scientifiques (IHÉS)</span>
								</span> <span class="language"> <span title="English">English</span>
								</span> <span class="releaseyear"> <span title="2015">2015</span>
								</span>
								<div class="clear"></div>
							</div>
						</div>

						<div class="searchresult-item" vocab="http://schema.org/"
							typeof="Movie">
							<iframe height="315" scrolling="no"
								src="http://av.tib.eu/player/<%=+recId[2]%>" frameborder="0"
								allowfullscreen></iframe>

							<div class="searchresult-title">
								<a href="<%=recId[2]%>" class="resultTitle" rel=""
									property="name"
									title="2/4 Singular support of coherent sheaves" lang="en">Recommendation
									3</a>
							</div>

							<div class="searchresult-subline">
								<span class="i-time duration"></span> <span class="publisher">
									<span property="publisher"
									title="Institut des Hautes Études Scientifiques (IHÉS)">Institut
										des Hautes Études Scientifiques (IHÉS)</span>
								</span> <span class="language"> <span title="English">English</span>
								</span> <span class="releaseyear"> <span title="2015">2015</span>
								</span>
								<div class="clear"></div>
							</div>
						</div>
					</div>
				</div>

				<div id="searchresult-overlay"></div>
			</div>
		</div>
		<div style="clear: both"></div>
		<div style="display: none;" id="scrollToTop">
			<i class="fa fa-angle-up"></i>
		</div>
		<footer>
			<div class="wrap">
				<div class="grid">
					<div class="grid-w14">
						<h5>Customer Service</h5>
						<a class="email"
							href="javascript:linkTo_UnCryptMailto('iwehpk6yqopkianoanreyaWpex:aq');">customerservice<span
							class="tib-mail-a"></span>tib<span class="tib-mail-b"></span>eu
						</a> <span class="phone">+49 511 762-8989</span> <span class="fax">+49
							511 762-8998</span>
					</div>
					<div class="grid-w14">
						<h5>Legal Notices</h5>
						<ul>
							<li class="footer-li"><a href="https://av.tib.eu/terms"
								title="Terms and conditions">Terms and conditions</a></li>
							<li class="footer-li"><a
								href="https://av.tib.eu/terms#privacy" title="Data protection">Data
									protection</a></li>

							<li class="footer-li"><a
								href="https://www.tib.eu/en/service/imprint/" target="_blank">Impressum</a></li>

						</ul>
					</div>
					<div class="grid-w14">
						<h5>Member of</h5>
						<a href="http://www.leibniz-gemeinschaft.de/" target="_blank"><img
							src="static/leibnizgemeinschaft.png" alt="Leibniz-Gemeinschaft"
							height="130" width="196"></a>
					</div>
					<div class="grid-w14">
						<h5>Follow us</h5>
						<ul class="footer-social-ul">
							<li class="footer-social-li"><a
								href="https://www.facebook.com/TIBUB" target="_blank"
								title="Facebook">Facebook</a></li>
							<li class="footer-social-li"><a
								href="https://plus.google.com/113807279920626112678"
								target="_blank" title="google+">Google+</a></li>
							<li class="footer-social-li"><a
								href="https://twitter.com/tibub" target="_blank" title="Twitter">Twitter</a></li>
							<li class="footer-social-li"><a
								href="https://www.youtube.com/user/TIBUBnet" target="_blank"
								title="YouTube">YouTube</a></li>
						</ul>
						<div class="clear"></div>
						<ul>
							<li class="footer-li"><a href="http://tib.eu/"
								target="_blank" title="TIB-Portal">go to TIB-Portal</a></li>
						</ul>
					</div>
				</div>
			</div>
			DEBUG<br>
			<%= logs %>

		</footer>
	</div>

	<script src="static/main.js"></script>

</body>
</html>