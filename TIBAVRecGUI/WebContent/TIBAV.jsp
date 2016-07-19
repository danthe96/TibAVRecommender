<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8" import="java.sql.*,org.jsoup.Jsoup,org.jsoup.nodes.Document,org.jsoup.select.Elements" %>

<%
	int[] recId = { 18564, 19017, 15907 };
	int video_id = 16350;
	String yovistourl = "http://blog.yovisto.com/james-clerk-maxwell-and-the-very-first-color-photograph/";
	String logs = "";
	String title = "TIB|AV Recommender System";
	String rec1 = "";
	String rec2 = "";
	String rec3 = "";
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
			title = titleResult.getString("title");
			titleResult.close();
			getTitle.close();
		} catch (SQLException e) {
			//response.sendError(HttpServletResponse.SC_NOT_FOUND);
			//return;
			logs += "\ncaught second sql exception: ";
			logs += e.getMessage();
		}
		try {
			PreparedStatement getTitle = db_con.prepareStatement("SELECT title FROM tibav.tibvid WHERE videoid=?");
			getTitle.setInt(1, recId[0]);
			ResultSet titleResult = getTitle.executeQuery();
			titleResult.next();
			rec1 = titleResult.getString("title");
			titleResult.close();
			getTitle.close();
		} catch (SQLException e) {
			//response.sendError(HttpServletResponse.SC_NOT_FOUND);
			//return;
			logs += "\ncaught third sql exception: ";
			logs += e.getMessage();
		}
		try {
			PreparedStatement getTitle = db_con.prepareStatement("SELECT title FROM tibav.tibvid WHERE videoid=?");
			getTitle.setInt(1, recId[1]);
			ResultSet titleResult = getTitle.executeQuery();
			titleResult.next();
			rec2 = titleResult.getString("title");
			titleResult.close();
			getTitle.close();
		} catch (SQLException e) {
			//response.sendError(HttpServletResponse.SC_NOT_FOUND);
			//return;
			logs += "\ncaught fourth sql exception: ";
			logs += e.getMessage();
		}
		try {
			PreparedStatement getTitle = db_con.prepareStatement("SELECT title FROM tibav.tibvid WHERE videoid=?");
			getTitle.setInt(1, recId[2]);
			ResultSet titleResult = getTitle.executeQuery();
			titleResult.next();
			rec3 = titleResult.getString("title");
			titleResult.close();
			getTitle.close();
		} catch (SQLException e) {
			//response.sendError(HttpServletResponse.SC_NOT_FOUND);
			//return;
			logs += "\ncaught fifth sql exception: ";
			logs += e.getMessage();
		}
		
		db_con.close();
		
	} catch (NumberFormatException e) {
		response.sendError(HttpServletResponse.SC_NOT_FOUND);
		return;
	}
	
	// YOVISTO blog article fetch & parse //
	
	Document doc = Jsoup.connect(yovistourl).get();
	Elements metaOgTitle = doc.select("meta[property=og:title]");
	Elements metaOgImage = doc.select("meta[property=og:image]");
	String yovistotitle = metaOgTitle.attr("content");
	String imageUrl = metaOgImage.attr("content");
	
%>

<!DOCTYPE html>
<html
	class="js flexbox flexboxlegacy canvas canvastext webgl touch geolocation postmessage no-websqldatabase indexeddb hashchange history draganddrop websockets rgba hsla multiplebgs backgroundsize borderimage borderradius boxshadow textshadow opacity cssanimations csscolumns cssgradients no-cssreflections csstransforms csstransforms3d csstransitions fontface generatedcontent video audio localstorage sessionstorage webworkers applicationcache svg inlinesvg smil svgclippaths"
	style="" xmlns="http://www.w3.org/1999/xhtml" lang="en">
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8">
<meta charset="UTF-8">
<title><%=title + " | TIB|AV Recommender System"%></title>
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
		<header class="header-up" style="border-bottom: 3px solid #af1414;">
			<div class="wrap">
				<span id="nav-toggle"></span>
				<img src="static/TIBAVrec.png" alt="TIB|AV Recommender System" style="bottom:0;right:0;height:128px;"></img>

				<div class="clear"></div>
			</div>
		</header>
		<div id="content">
			<div class="wrap">
				<div class="detail-head">
					<h1 property="name"><%=title%></h1>
				</div>
			
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
									title="2/4 Singular support of coherent sheaves" lang="en"><%=rec1%></a>
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
									title="2/4 Singular support of coherent sheaves" lang="en"><%=rec2%></a>
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
									title="2/4 Singular support of coherent sheaves" lang="en"><%=rec3%></a>
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
						
						<div class="searchresult-item" vocab="http://schema.org/" typeof="Article">
							
							<a href="<%=yovistourl%>" class="resultTitle" rel="" property="name" title="yovisto blog recommendation: <%=yovistotitle%>" lang="en">
								<img src="<%= imageUrl %>" height="315" scrolling="no"></img>
							</a>

							<div class="searchresult-title">
								<a href="<%=yovistourl%>" class="resultTitle" rel=""
									property="name"
									title="yovisto blog recommendation: <%=yovistotitle%>" lang="en"><%=yovistotitle%></a>
							</div>
							
							<div class="searchresult-subline">
								<span class="publisher">
									<span property="publisher"></span>
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
				<div class="clear"></div>
					<a href="https://av.tib.eu/"
					title="Home"> <img src="static/TIB_Logo_AV-Portal.png"
					alt="TIB-AV" height="105" width="357"></a>
					<p style="float:right;">Julius Rudolph<br>Nils Thamm<br>Daniel Thevessen<br>Lennart Lehmann</p>
			</div>

		</footer>
	</div>

</body>
</html>