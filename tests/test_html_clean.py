import re
from bs4 import BeautifulSoup
import json


def clean_forum_text(html_text: str) -> str:
    """
    Cleans messy boards.ie / similar forum HTML with heavy embedjson data.
    Returns clean, readable text without duplicates.
    """
    if not html_text or not isinstance(html_text, str):
        return ""

    soup = BeautifulSoup(html_text, "html.parser")

    # Remove script, style, etc.
    for tag in soup(["script", "style"]):
        tag.decompose()

    collected_texts = []

    def extract_from_embedjson(json_str: str) -> str:
        """Safely extract text from data-embedjson"""
        try:
            # Fix common escaping issues before parsing
            fixed = json_str.replace('&quot;', '"') \
                .replace('\\"', '"') \
                .replace('\\\\', '\\')

            data = json.loads(fixed)

            texts = []

            # Extract from body (HTML)
            if isinstance(data.get("body"), str):
                if data["body"].startswith("%PDF"):  # Skip PDF garbage
                    return ""
                body_soup = BeautifulSoup(data["body"], "html.parser")
                texts.append(body_soup.get_text(" ", strip=True))

            # Extract from bodyRaw (rich text structure)
            if isinstance(data.get("bodyRaw"), str):
                try:
                    raw_list = json.loads(data["bodyRaw"])
                    for item in raw_list if isinstance(raw_list, list) else []:
                        if isinstance(item, dict) and item.get("children"):
                            for child in item["children"]:
                                if isinstance(child, dict) and child.get("text"):
                                    texts.append(child["text"].strip())
                except:
                    pass

            return " ".join(t for t in texts if t).strip()
        except:
            return ""

    # Walk through all elements and extract text
    for element in soup.find_all(["p", "div", "span", "blockquote", "li"]):
        embed_json = element.get("data-embedjson") or element.get("data-embed-json")

        if embed_json:
            extracted = extract_from_embedjson(embed_json)
            if extracted:
                collected_texts.append(extracted)
        else:
            text = element.get_text(" ", strip=True)
            if text:
                # Skip lone URLs (they're usually just embed placeholders)
                if re.match(r'^https?://\S+$', text):
                    continue
                collected_texts.append(text)

    # Final cleaning
    full_text = " ".join(collected_texts)
    full_text = re.sub(r'\s+', ' ', full_text)  # normalize spaces
    full_text = re.sub(r'\s+([.,!?])', r'\1', full_text)  # fix punctuation spacing
    full_text = full_text.strip()

    return full_text

def parse_html(text):
    """
        Parse cleaned HTML and extract text.
        Removes:
        - ALL js-embed blocks (including embedded PDFs/JSON)
        - links
        - scripts/styles
        """
    try:
        soup = BeautifulSoup(text, "lxml")

        # REMOVE ANYTHING WITH data-embedjson (most reliable)
        for tag in soup.find_all(lambda t: t.has_attr("data-embedjson") or t.has_attr("data-embed-json")):
            tag.decompose()

        # ALSO remove js-embed class (belt + braces)
        for tag in soup.find_all(
                lambda t: t.name in ["div", "span"]
                          and t.get("class")
                          and any("js-embed" in c for c in t.get("class"))
        ):
            tag.decompose()

        # Remove scripts and styles
        for tag in soup(["script", "style"]):
            tag.decompose()

        # Remove links
        for a in soup.find_all("a"):
            a.decompose()

        return soup.get_text(" ", strip=True)

    except Exception as e:
        print(f"BeautifulSoup parsing failed: {e}")
        return text



text = r"""<div class="js-embed embedResponsive" data-embedjson="{&quot;recordID&quot;:123540519,&quot;recordType&quot;:&quot;comment&quot;,&quot;body&quot;:&quot;&lt;p&gt;&lt;a href=\&quot;https:\/\/www.boards.ie\/discussion\/comment\/123540481#Comment_123540481\&quot;&gt;https:\/\/www.boards.ie\/discussion\/comment\/123540481#Comment_123540481&lt;\/a&gt;&lt;\/p&gt;&lt;p&gt;Well I always knew British companies were making billions from selling weapons of genocide to Israel. But I didn't think they'd have to train the IDF too. &lt;\/p&gt;&quot;,&quot;bodyRaw&quot;:&quot;[{\&quot;type\&quot;:\&quot;rich_embed_card\&quot;,\&quot;children\&quot;:[{\&quot;text\&quot;:\&quot;\&quot;}],\&quot;dataSourceType\&quot;:\&quot;url\&quot;,\&quot;url\&quot;:\&quot;https:\\\/\\\/www.boards.ie\\\/discussion\\\/comment\\\/123540481#Comment_123540481\&quot;,\&quot;embedData\&quot;:{\&quot;url\&quot;:\&quot;https:\\\/\\\/www.boards.ie\\\/discussion\\\/comment\\\/123540481#Comment_123540481\&quot;,\&quot;embedType\&quot;:\&quot;link\&quot;}},{\&quot;type\&quot;:\&quot;p\&quot;,\&quot;children\&quot;:[{\&quot;text\&quot;:\&quot;Well I always knew British companies were making billions from selling weapons of genocide to Israel. But I didn't think they'd have to train the IDF too. \&quot;}]},{\&quot;type\&quot;:\&quot;p\&quot;,\&quot;children\&quot;:[{\&quot;text\&quot;:\&quot;\&quot;}]}]&quot;,&quot;format&quot;:&quot;rich2&quot;,&quot;dateInserted&quot;:&quot;2025-06-13T13:06:28+00:00&quot;,&quot;insertUser&quot;:{&quot;userID&quot;:52487,&quot;name&quot;:&quot;NIMAN&quot;,&quot;url&quot;:&quot;https:\/\/www.boards.ie\/profile\/NIMAN&quot;,&quot;photoUrl&quot;:&quot;https:\/\/us.v-cdn.net\/6034073\/uploads\/userpics\/X1O1HR3D7PBV\/nCE1CPPLHKB67.jpg&quot;,&quot;dateLastActive&quot;:&quot;2026-01-31T17:59:31+00:00&quot;,&quot;banned&quot;:0,&quot;punished&quot;:0,&quot;private&quot;:false,&quot;label&quot;:&quot;✭✭✭✭&quot;,&quot;labelHtml&quot;:&quot;✭✭✭✭&quot;},&quot;displayOptions&quot;:{&quot;showUserLabel&quot;:false,&quot;showCompactUserInfo&quot;:true,&quot;showDiscussionLink&quot;:false,&quot;showPostLink&quot;:false,&quot;showCategoryLink&quot;:false,&quot;renderFullContent&quot;:false,&quot;expandByDefault&quot;:false},&quot;url&quot;:&quot;https:\/\/www.boards.ie\/discussion\/comment\/123540519#Comment_123540519&quot;,&quot;embedType&quot;:&quot;quote&quot;,&quot;embedStyle&quot;:&quot;rich_embed_card&quot;}">     <a href="https://www.boards.ie/discussion/comment/123540519#Comment_123540519">         https://www.boards.ie/discussion/comment/123540519#Comment_123540519     </a> </div> <p>seeing as the UK has sold about £500m in arms in the last ten years, I’d be surprised if it is billions, especially as the UK accounts for less than 1% of arms sales to Israel. <br/></p><p><span class="js-embed embedResponsive inlineEmbed" data-embedjson="{&quot;body&quot;:&quot;%PDF-1.7\r\n%µµµµ\r\n1 0 obj\r\n&lt;&gt;\/Metadata 77 0 R\/ViewerPreferences 78 0 R&gt;&gt;\r\nendobj\r\n2 0 obj\r\n&lt;&gt;\r\nendobj\r\n3 0 obj\r\n&lt;&gt;\r\nendobj\r\n4 0 obj\r\n&lt;&gt;\/ExtGState&lt;&gt;\/XObject&lt;&gt;\/ProcSet[\/PDF\/Text\/ImageB\/ImageC\/ImageI] &gt;&gt;\/MediaBox[ 0 0 595.32 841.92] \/Contents 5 0 R\/Group&lt;&gt;\/Tabs\/S\/StructParents 0&gt;&gt;\r\nendobj\r\n5 0 obj\r\n&lt;&gt;\r\nstream\r\nxÍ][OG~Gâ?ô#DKS÷®¬fÙµ¬Ý$ÄÆíÚ8Ñ*ÿ~ë^§nÌº\ttOw×©sýÎ¥'Ë¯·ß]½¹í=;\\ÞÞ^½ùpý¶ûéðòæË\/~¹&gt;üáêýÇÏW·o&gt;¾úã×[}êùõÕÛë¯E·:Zw¿íî éÿ¤p:&gt;òN2Ü¤ûz½»óú»îóîÎêrwçðwtïvwô¥¨ÃÝ@zDX7è»DwùI]ôý+Ù½ÿ]=·{oFwôýîÎO{]·@ãO±×íÿÒ]þ}wçX=ÿ»;¥G_z~õ{.öí©zEGU÷D&gt;MÒÑã'Ê5!ô\rO4&gt;ôâ\nÉ³§I{úD)SÏy¢V@P?&gt;QybÜeg?îó½(7¼ÀãÞú³³¥:ùRý;zp¨Ð©Ð|ªÑÔ©G¢Î¾ùiïüÉV×Xø1ÅÛbB¤g¥rî{\/,N^i]îcfÅör0]r\nË='\/÷\tV«ð½ËçJ÷1²KKúô\\Q§È_xr±­R­½Æ½z Ù´EB6Á¬'¼EöcJ¸Ä{&gt;%]Ýñùº;làÌÕÍííÍ§6Ô&lt;¹¹¹jö&gt;Pé,Ùû aåþñæ?WJn\rÎ\\î{oõúÔ'­3Ý-¿ \r¸§²EÛPsÂßü¡\rç÷kÇ÷¥åÝ;Ãµõ:6Üú´ïúYÿø}jÎÉÓ=ç8îy°³ iR½GÓrGyIÜ¢á¯g­\&quot;_c¯µbaÅ¥&gt;êuÝú7-¨¦¤AÈ\\noûô:&gt;a¥5mÉN²'D¡FHýÔ«¿ûÎ-ü¹¬G\n4\nI0{%ïFÚÅöHéÖzöy@£\\¥£êv©R¿2¿:?}¡T\&quot;Õ@\tòå¿3è.´ªÁí`¬-³ÇÇG¬Î?L6 ¥ 'âÊÀú\n8Ñ=»Ð?\/éøß:Å3\/5Ó&amp;§QÖcÖ j5ùjéûê«©\r{çKmÖ§\/Ôg+ÅOÏÌÁéï§&amp;åôÌ¦ö¨ô,¯ÕV^¾Ô?;S§»³lïèB3çE÷J_ðZÿÀ:éé¾ôùCÅùA®2FZä&gt;;UÖp²Ôb)¦Â`¹­ËkÍáÿj3æÊ]&gt;P&lt;ÚÓ6øuÌáàñ¤´2tîW§UN¼ÖÐ²µÖ´2Ð~·=WÝIHW¯gÚpDk­U Ätïoµ®½Ñ ØèÙ¯j÷76³P?¾øóþ¤q,ÛÜÇØ(îåFkedÔX£¡Ç¼¼ÀBî¬~¾ù$^ÿF·\nN?]½¿Vìè¦ó«;|õåê³\\çëÓ£]}~ßí]&gt;ø~µ?m&amp;)^©Ò0-Ä^jÑ«ÃÒS&lt;µ\&quot;©ÅS'`âfL#äÇYKªÛùã¬E°x¼µT¨{´µ¤|´µ(aÚ[&lt;ÎZÊ?ÚZ£x´µtØÇ¥óèÎ¯4NüS?fÃ$Ñ?øÄdL{í­¶\\s´xNG+Ôå÷!ÌIø&gt;DÑ9RÞÆ®X(6'Qmú5¢øD!q?E3ÅGr?Eæ$JW¥¢®¯4ÄùÚ½ú ÜR¨«_·hC«S±kä¬ñÞªqªh*®rùªgÑ!¶Zõ7cª~sl³cµ,UÇëÔ§ûXÚßú#6.ü»;úHº§xþ­Ü.¢ïdKóËÜ4¸eWî7XÇÜ¸À|îÉU÷Kw9].0¸[¸Ú­&amp;QÑÜ)ìGêrvAVæ*Ïv¢ÿ+Ì§C|&gt;ãî·ç`|¡^±ìYZi*¹­Ãsõæ^ùÅ×n=ai&amp;5@GÊ´í×æ&lt;»Ftñ£lª.×T\/\rãçÏÊ2Åìm\rá¨Ö¼en£+f(ÇØPm.%-V&lt;·Ð\&quot;èË2U6Ï¯ÙK ZCÊ5õPYÓ.¯BPe$[W6æqAÏ.\rÓÔ&lt;ÝdPl×â¥º+K²í\téÇÐ\\âd¼ðÁ|Ær§àh1^à85V¸\r'1£.ÚËª.4déiPèYwn4xV8Õ(}H¢è¤Ôwó4Q?%£%´âN3µ n+ÚWð¦ÚFæ½XÚßáðDI¬º§úÇF{{W7,0®¨«!ä$qÊ[g¹A\\1¢dIkkp¼±ÂñLï­çrAj¢ \&quot;ÂøA:VIÑ.âÕul«mÎjÇ&amp;?¿YSä-¯ VB4­ócÀ«r\n\r(Z\&quot;BVs.Üb§1F¯à]FMãCªôJÆgñ4o¨qÅ|\r12ðàh\&quot;Ñpr*¹ÙB6hyâþ»|70¶ÙýÃÉqØ ÏØn:²2zàD6xCè(\\¼Ö$\tÎ¢#$Hbë,ñIRéÁ±ÿN[cànm%°Æh_gknxlj\\\/©((äÜ±£ïÄ\&quot; ,j;&lt;±r20]ÚU~\n@@%O`\&quot; ïÀ*lltkeXÌ%Ôtó(e4.Æj(8Á\rjN5Dq­ù&amp;0þx&gt;²×PoãNÕ6ùS·_¨YÈoV@k3Ò¹ 3\/ G`}mBéQÍ­]PÇ©ø´p`û«Ôïùàáö2¡5ØÒ²÷§É¿L¥&amp;IêêùµbhËüóK`®i3;wÔÖ\&quot;¯90RZÍÓrÖ þÎò\r\tÀ×e©@  µh8¼Ð%Æ¬\&quot;Á×8±k3àrüYÈq±Õìá$Ø×·#FÞâýZõ&amp;Ì²xèôA\&quot;)ûÍIàÛ×n'8$Tô:¤êk1t¯æ)1µ)Ðÿ*ÿ&lt;º¥.\/Qí=¦ÖßOJ.f&amp;7hÑÄëq¤GÉÚì¾ejçí*¤Î\tÒ(DøP\tKh'&gt;ÆaYÅ»´øÎkú0×2e#Ñ\&quot;A$'^*sñTòÇ4QOñÝvà%@ (¡}ñqÐ&lt;¤V»» AÙ\\nõ¬²(ËÆh`jÕÔéÄÊ2ºß&lt;ùY£4Ë N·ùFÖQ+VïÅô%\nBqÏi=[ÕælÙ2L+×VdMÑ´m)Ç:\&quot;ë(ú\r­7ð¢XÜïI¬T¤µ-kÖ.4Ï×5zç½³-êÅçÄßÃ¤! ahAÊÈ±\r¿û98,á\\H\&quot; ;eq¡l±@&lt;ã æ54I  0é\&quot;ÑîJ®çø*îªYOÑ[q\&quot;ÍáqtiQÚN3ÝÊ|Û\nWT:×jÕ W{ùÐª|¥kõÀ@¨ÏKfA!¸(3° kí´ÍÖ¬ æÕ\/zc%¡ØV# ñöQÀå·ùåë§o6;«qhIxãÇLÂU4±^AÉ­ÓðÈ\\ï¬­Ù°\n+V òÞ\n?çð7`C¶ÒR6©³aÎ3:Y@ËK3ü1¤ÄÜe´ä§h&amp;cAF¢êø­VÇ8=o«XÌùÍ£í\\ê·}+Ý¨3Êb1aËÉQ~AÜÌ5ZÙX£× *QFk³7U²Õ&lt;¾`7Zá _,bàsáÕ~9'§IUò®þ|rIV¥Ë¬\&quot;6#Á% 4ï¸7Öóàdíb¬gUíÚ4hÔJj\\Qi¼NÊ¸°ÈBÊû²¬Y\\Q¿àÅEQd3v ³tëîòÐmD1-Æ$ªü\\\nVÛÀ-u®YÞ}O´j°°.V¡Yáïîl§ã3Å(EýpgÉÙ`Ä¦\&quot;$\n1ú¸VêN¨áô´kG I[õ:fYJ5ä.&quot;,&quot;url&quot;:&quot;https:\/\/data.parliament.uk\/DepositedPapers\/Files\/DEP2025-0286\/Military_Cooperation_with_Israel-letter_to_Shockat_Adam_MP.pdf&quot;,&quot;embedType&quot;:&quot;link&quot;,&quot;name&quot;:&quot;&quot;,&quot;embedStyle&quot;:&quot;rich_embed_inline&quot;}">     <a href="https://data.parliament.uk/DepositedPapers/Files/DEP2025-0286/Military_Cooperation_with_Israel-letter_to_Shockat_Adam_MP.pdf" rel="nofollow noopener ugc">         https://data.parliament.uk/DepositedPapers/Files/DEP2025-0286/Military_Cooperation_with_Israel-letter_to_Shockat_Adam_MP.pdf     </a> </span> </p><p>here’s the full response to the parliamentary question  </p>"""
print(parse_html(text))