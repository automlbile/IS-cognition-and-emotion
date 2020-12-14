library(stringr)
library(dplyr)
library(xlsx)
library(rJava)
library(xlsxjars)
library(igraph)
library(RSQLite)
library(DBI)
library(ggplot2)
 

starttime = Sys.time()
setwd('D:/projects/Rs/cogem/')
stagedate<- str_replace_all(as.character((as.Date(starttime)) ),'-','')
stagedate<-'20200909'  #临时处理，因为程序尚未写完

##连接数据库
con<-dbConnect(SQLite(),"cogem.db")
dirnames = list.dirs(path="./data",full.names=TRUE)##下载的数据文件目???

pathname<-getwd() #当前工程路径
fullname<-NULL #保存文件绝对路径
tmptbl<-NULL   


###第一步，初始化临时文件表，把下载下来的数据存放到数据库临时文件表
  #清表
  dbRemoveTable(con,'dataraw')
  rawsql<- paste0("create table  dataraw", " (id integer, content varchar(3800), times int, addtime datetime default CURRENT_TIMESTAMP) "    )
  dbExecute (con,rawsql)
 
###第二步 导入数据到临时表
    filenames=list.files(path=dirnames,pattern="*.txt",include.dirs = FALSE)#取出单个目录下的数据文件
    filenames<-str_sort(filenames, numeric = TRUE,decreasing = FALSE)
    sdirname<-substring(dirnames,3,nchar(dirnames))
    ttimes<-0
    for( filename in filenames){ #每个数据文件循环
      
      fullname<-paste0(pathname,"/",sdirname,"/",filename)
      print(paste("正在执行",fullname,sep=" "))
      
      #组合导入数据
      tfile<-  readLines(fullname,encoding = "UTF-8",warn=FALSE)
      tnumber<- seq(1:length(tfile))
      ttimes<-ttimes+1
      taddtime<-Sys.time()
      tdata<- data.frame(id=tnumber,content=tfile,times=ttimes, addtime=taddtime)
      dbWriteTable(con, "dataraw",tdata,append=TRUE)
      
    }
   
    
    
####第三步，把rawdata整理称为paper系列宽表
    dbRemoveTable(con,'papers')
    prosql<- paste0("create table  papers", " (ti varchar(2000),au varchar(6000),so varchar(600),py varchar(20), ab text,de varchar(1000),id int, DOI varchar(300), UTI varchar(300)) "    )
    dbExecute (con,prosql)  #论文表
 
    v_counter<- 0
    loopsql<- "SELECT  id  as nowid, content, substr(content,1,2) as linetype ,times as ttt FROM  dataraw    ORDER  BY times , nowid"
    totalsets<-dbGetQuery (con,loopsql)
   
    for( i in 1:length(totalsets$nowid)){ #每条记录循环处理
       if(i%%1000 ==0) print(paste0(i,Sys.time()))
       tset<-totalsets[i,]
       nowid<-tset$nowid
       content<-tset$content
       linetype<-tset$linetype
       ttt<-tset$ttt
       
       if( tset$linetype=='PT'){   #标识新文章
         v_counter = v_counter+1
         dbExecute (con,paste0("insert into papers(id) values(",v_counter, " )" ) )
       }
       
       
       else if( tset$linetype=='TI'){  #标题
          content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
          tisql<-paste0( "select id nid, content, substr(content,1,2) linetype from dataraw where substr(content,1,2)='  ' and id > ",nowid ,
                         "  and id < (select min(id) from dataraw where id>", nowid," and substr(content,1,2)!='  ' and times=", ttt,")",
                         " and times=",ttt)
          tiadds<-dbGetQuery(con, tisql)
           
          for( j in 1:length(tiadds$nid))
          {
            if(! is.null(tiadds)){ content<-paste(content,trimws(tiadds[j,"content"]),sep = " ")}
          }
          content<- gsub("'","" , trimws( content))  
          dbExecute(con,paste0("update papers set ti= '",content," ' where id=",v_counter))
       
       }
        
       
       else if( tset$linetype=='AU'){  #作者
         content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
         ausql<-paste0( "select id nid, content, substr(content,1,2) linetype from dataraw where substr(content,1,2)='  ' and id > ",nowid ,
                        "  and id < (select min(id) from dataraw where id>", nowid," and substr(content,1,2)!='  ' and times=", ttt,")",
                        " and times=",ttt)
         auadds<-dbGetQuery(con, ausql)
         
         for( j in length(auadds$nid))
         {
           if(! is.null(auadds)){content<-paste(content,trimws(auadds[j,"content"]),sep = "; ")}
         }
         content<- gsub("'","" , trimws( content))  
         dbExecute(con,paste0("update papers set au= '",content," ' where id=",v_counter))
         
       }
       
       
       else if( tset$linetype=='SO'){  #期刊名
         content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
         sosql<-paste0( "select id nid, content, substr(content,1,2) linetype from dataraw where substr(content,1,2)='  ' and id > ",nowid ,
                        "  and id < (select min(id) from dataraw where id>", nowid," and substr(content,1,2)!='  ' and times=", ttt,")",
                        " and times=",ttt)
         soadds<-dbGetQuery(con, sosql)
        
         for( j in length(soadds$nid))
         {
           if(! is.null(soadds)){content<-paste(content,trimws(soadds[j,"content"]),sep = " ") }
         }
         content<- gsub("'","" , trimws( content))  
         dbExecute(con,paste0("update papers set so= '",content," ' where id=",v_counter))
         
      }
       
       
       else if( tset$linetype=='PY'){  #出版日期
         content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
         dbExecute(con,paste0("update papers set py= '",content," ' where id=",v_counter))
      }
       
       
       else if( tset$linetype=='DI'){  #doi号
         content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
         dbExecute(con,paste0("update papers set DOI= '",content,"' where id=",v_counter))
       }  
       
       
       else if( tset$linetype=='UT'){  #doi号
         content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
         dbExecute(con,paste0("update papers set UTI= '",content," ' where id=",v_counter))
       }  
    
       
       else if( tset$linetype=='AB'){  #摘要
         content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
         dbExecute(con,paste0("update papers set ab= '",content," ' where id=",v_counter))
       }
    
       
       else if( tset$linetype=='DE'){  #关键字
         content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
         desql<-paste0( "select id nid, content, substr(content,1,2) linetype from dataraw where substr(content,1,2)='  ' and id > ",nowid ,
                        "  and id < (select min(id) from dataraw where id>", nowid," and substr(content,1,2)!='  ' and times=", ttt,")",
                        " and times=",ttt)
         deadds<-dbGetQuery(con, desql)
         
         for( j in length(deadds$nid))
         {
           if(! is.null(deadds)){ content<-paste(content,trimws(deadds[j,"content"]),sep = " ")}
         }
         content<- gsub("'","" , trimws( content))  
         dbExecute(con,paste0("update papers set de= '",content," ' where id=",v_counter))
         
       }

    }
    
    
########第四步，按照期刊筛选需要的论文
    dbRemoveTable(con,'is_select')
    prosql<- paste0("create table  is_select", " (newid int ,UTI varchar(200),DOI varchar(300), ti varchar(2000),au varchar(4000),so varchar(600),py varchar(20), ab text,de varchar(1000),id int, source int ) "    )
    dbExecute (con,prosql)  #论文表
    
    journals<-read.csv(paste0(getwd(),"/isjournal.csv") ,header = FALSE)
    names(journals) = c('NAME','BNAME','STAT')
    dbRemoveTable(con,'isjournal')
    dbWriteTable(con, "isjournal",journals,append=TRUE)
    
    i1sql<- paste0(" insert into  ","is_select", "(id,UTI,DOI,ti,au,so,py,ab,de,source)" )
    i2sql<- paste0(" select   a.id ,a.UTI,a.DOI, a.ti, a.au, a.so, a.py, a.ab, a.de, ",1)
    i3sql<- paste0( " from papers  a ,isjournal b where  b.stat<>'0' and upper(trim(a.so))=upper(trim(b.name)) " ) 
    iinsertsql<-paste0(i1sql,i2sql,i3sql )
    dbExecute(con,iinsertsql)
    
    print("finish step 4")
    
########第五步， 引文处理
    dbRemoveTable(con,'is_cite_all')
    prosql<- paste0("create table  is_cite_all", " ( newid int, ref varchar(2000) ) "    )
    dbExecute (con,prosql)  #论文表
  
    #插入信管引文表  
    trs1<- dbGetQuery(con,"select id, content,times from dataraw where substr(content,1,2)='TI' order by times, id ")
    trs2<- seq(1:length(trs1$content))
    trs3<- data.frame(trs2, trs1)
    names(trs3)<-c('num','id','content','times')
    isrs<- dbGetQuery(con,"select id number from is_select")
    trs<- trs3[trs3$num %in% isrs$number,]
   
    vcounter=0
    #每一篇论文循环处理
    for(num in trs$num)
    
    {   
      vcounter<-vcounter+1 #vcounter用于重新给文章编号，使得文章记录和引文记录能够进行关联
     print(paste0("now do ", vcounter))
    ##先找引文的起始记录
    ssql1<-   "SELECT id st_num  FROM  dataraw"
    ssql2<-paste0(" where id >" ,trs$id[trs$num==num]) 
    ssql3<-paste0(" and times=" ,trs$times[trs$num==num] ) 
    ssql4<-" and  substr(content,1,2)='CR' order by id limit 0, 1"
    ssql<-paste0(ssql1,ssql2,ssql3,ssql4)
    st_t<-dbGetQuery(con,ssql)
    st_t
    
    ##再找引文的结束记录
    esql1<-    "SELECT  MIN(id)  end_num  FROM dataraw"
    esql2<-paste0(" where id >" ,st_t  ) 
    esql3<-paste0(" and times=" ,trs$times[trs$num==num]  ) 
    esql4<-" and  substr(content,1,2)!='  ' "
    essql<- paste0(esql1,esql2,esql3,esql4)
    essql
    end_t<-dbGetQuery(con,essql)
    end_t
    ##起始和结束记录之间的即为该篇文章的引文
    rssql0<-paste0( " insert into  is_cite_all  select  ",vcounter )
    rssql1<-  ", TRIM(substr(content,3,length(content))) from dataraw " 
    rssql2<-paste0(" where id>=" ,st_t  ) 
    rssql3<-paste0(" and    id<" ,end_t  ) 
    rssql4<-paste0(" and times=" ,trs$times[trs$num==num]  ) 
    rssql<- paste0(rssql0,rssql1,rssql2,rssql3,rssql4)
    dbExecute(con,rssql)
    
    ##更新文章的编号信息
    upsql<-paste0("update is_select set newid=",vcounter," where id=", num)
    dbExecute(con,upsql)
  }
    
    
    
###第六步，精炼数据 剔除不符合要求的数据 
    
    dbRemoveTable(con,"is_select_final");
    dbRemoveTable(con,"is_cite_all_final");
    prosql<- paste0("create table  is_select_final", " (newid int ,UTI varchar(200),DOI varchar(300), ti varchar(2000),au varchar(4000),so varchar(600),py varchar(20), ab text,de varchar(1000),id int, source int ) "    )
    dbExecute(con,prosql)
    prosql<- paste0("create table  is_cite_all_final", " ( newid int, ref varchar(2000) ) "    )
    dbExecute (con,prosql)  
    
    dbExecute(con," INSERT INTO is_select_final SELECT * FROM is_select WHERE (ti,newid) IN (SELECT ti ,MIN(newid) FROM is_select GROUP BY ti )"); #精炼数据,去重 
    dbExecute(con," INSERT INTO is_cite_all_final SELECT *  FROM is_cite_all")
    
     
    dbGetQuery(con,"select count(*) from is_cite_all")
###第七部分，期刊数据描述性分析
   
    #(1)期刊年限分布
    
    years<-seq(1996,2019)
    articles<-c(9,9,18,6,42,33,34,46,23,43,48,73,72,86,90,111,110,120,155,182,174,182,172,223)
    sum(articles)
    df <- as.data.frame(cbind(years,articles))
   # jpeg(file = "outputs/annual_journal1.jpeg", res=600)
    tiff(file="outputs/annual_journal.tif",compression="lzw",units="in",res=600,pointsize=8,height=7.68*0.75, width=13.66*0.75)
   # jpeg(file = "outputs/annual_journal1.jpeg", height=768*6,width=1366*6, res=100*6)
    ggplot(df, aes(x=years, y=articles)) + geom_line(size=1.5)+  geom_point(size=4, shape=21, fill="blue",data=df)+geom_text(colour='black',aes(label=articles), hjust = 0,vjust=0,  nudge_y = -10,size=4) +theme(axis.title.x =element_text(size=12), axis.title.y=element_text(size=12)) # hjust=0,nudge_x=0.5
    dev.off()
    
     
    
    
    #(2)期刊来源分布
     
    
    nsql <- " SELECT NAME, count(*)  FROM  IS_SELECT_FINAL  a, isjournal b where  upper(trim(a.so))=upper(trim(b.name))  group by  name order by count(*) desc  limit 0, 15  "
    sjournals<-dbGetQuery(con,nsql)
    names(sjournals)<- c ('name','number')
    jj<- data.frame(name='OTHERS',number=476)
    sjournals<-rbind(sjournals,jj)
    names(sjournals)<- c ('name','number')
    par(mar = c(2, 2, 0, 0))
    tiff(file="outputs/journal_publish.tif",compression="lzw",units="in",res=600,pointsize=8,height=7.68*0.75, width=13.66*0.75)
    plt<- barplot(rev(sjournals$number),horiz=T,xlim=c(-600,500),axes='F',col='lightblue' )
    text(plt,x=-350,label=rev(sjournals$name))
    text(plt,x=-50,label=rev(sjournals$number))
    axis(3,c(0,100,200,300,400,500),c(0,100,200,300,400,500))
    dev.off()
  
###第八部分，引文分析
    
    #（1）读取文章记录
    ssql<- " select NEWID,DOI, TI ,SO, PY ,au from is_select_final"
    #读取每篇文章的DOI 并顺序排列，后面和引文进行比对，从而计算出个文章的citation number
    articles<-dbGetQuery(con,ssql)
    articles$DOI<-str_to_upper(articles$DOI)  #统一大写处理
    articles<- articles[order(articles$newid), ] #按照NEWD 排序
     
    
    #(2)获取文章的引文记录
    citesql<- " select NEWID,REF from is_cite_all_final " 
    cites<-dbGetQuery(con,citesql)
     
    doi_f <- function(text ) {          #获取每篇引文的DOI信息函数
      text1<-str_to_upper(as.character(text))   #统一大写处理
      st<-str_locate(text1,', DOI')[2]+2
      rs<-str_replace_all(str_sub(text1,st),"DOI ","")
      return(rs)
    }
    
    refs<-   cites$ref
    DOIS<-as.vector(sapply(refs ,doi_f))   #返回所有引文的DOI
    tcitearticles<-data.frame(NEWID= cites$newid, REF=cites$ref,DOI=DOIS, stringsAsFactors = FALSE)
    
    
    #(3) 构建每一篇论文的相互引用关系  循环处理每篇文章的引用，如果引用了这篇文章， from是引用者，to是被引用
    tciteall <- data.frame( from = character(0), to = character(0), stringsAsFactors = FALSE)
    
    #构建每一篇论文的相互引用关系
    for( i in 1:lengths(articles) ){   #所有从wos筛选的文章都要循环
      print(paste('cicire',i,sep=" "))
      tto<-articles[i,1]           # 取文章的编号（作为被引）
      print(tto)
      tdoi<-articles[i,2]          #取文章的DOI（作为被引）
      citethis<-as.vector(na.omit(tcitearticles$NEWID[tcitearticles$DOI==tdoi])) #找出所有引用了这篇文章的NEWID
      tcitethis<-NULL
      if(length(citethis)>0)
      {
        tcitethis<-data.frame(from=citethis,to=tto)  #生成引用当前文章的边记录
      }
      
      tciteall<-rbind(tciteall,tcitethis)  #添加的全部边中
      
    }
    
     
    #（4）构建引文矩阵
    crnames<-seq(1:2061)         #paste0('A',seq(1:2061))
    citematrix<- matrix(data=0,  #初始化没有边， 全零
                        nrow =  dim(articles)[1], # 行取文章篇数
                        ncol = dim(articles)[1],  #  列取文章篇数
                        byrow = FALSE, dimnames = list(crnames,crnames))
    for(i in 1:dim(tciteall)[1])  #对每一条边，在引文矩阵中赋值为1
    {
      from <-tciteall[i,c('from')]
      to <- tciteall[i,c('to')]
      citematrix[from,to]=1
      
    }
    
    #輸出  citematrix
    write.csv(citematrix, "outputs/citematix.csv" )
    
    
    
    #导出引用明细，这在后续的网络图中就是边的信息
    write.table(tciteall, file = "outputs/cite_detail.csv", sep = "," , col.names = TRUE,row.names = FALSE,  qmethod = "double")
    
    # 导出点的信息，
    dot1<- data.frame(dot=tciteall$from, stringsAsFactors = FALSE)
    dot2<-data.frame(dot=tciteall$to, stringsAsFactors = FALSE)
    dot3 <- rbind(dot1,dot2) %>% group_by(dot) %>% filter(row_number() == 1) %>% ungroup()   #删除重复行，保留全部点的单个信息
    write.table(dot3, file = "outputs/citedots.csv", sep = "," , col.names = TRUE,row.names = FALSE,  qmethod = "double")
    

###第九部分，SNA分析
    
    #（1）全部顶点网络图构建
    
    #邻接矩阵方式构建图，全部2061个定点，主要为了获取网络图密度  A
    options(digits=6)
    g <- graph.adjacency(citematrix,mode="directed")
    #plot(g)
    vcount(g)  #顶点统计
    ecount(g)   #边统计
    graph.density(g) #密度
    
    
    
    # （2）剔除孤立点后，使用边和点的方式构建网络图  B
    library(igraph)
    #导入边数据 
    edges <- read.table("outputs/cite_detail.csv", header=T, sep=',') #导入边数据，里面可以包含每个边的频次数据或权重
    edges$from<-edges$from  
    edges$to<-edges$to  
    
    #导入顶点
    vertices <- read.table('outputs/citedots.csv', header=T, sep=',') #导入节点数据，可以包含属性数据，如分类
    
    #生成引文图 
    gg <- graph_from_data_frame(edges, directed = TRUE, vertices=vertices) #directed = TRUE表示有方向,如果不需要点数据，可以设置vertices=NULL
    #com = walktrap.community(gg, steps = 10)  #调用分群算法划分子群
   
    #引文图各项指标
    
    vcount(gg)#顶点统计
    ecount(gg)#边统计
    
    #社群发现
    com =clusters(gg,mode="weak")
    
    #入度中心度数据
    par(mar = c(2, 2, 0, 0))
    V(gg)$dte = degree(gg, mode="in")   #计算入度中心度 in out all
    png("outputs/net_degreeness.png", width = 500, height = 500)
    plot(V(gg)$dte)
    dev.off()
    
    #中介中心度
    par(mar = c(2, 2, 0, 0))
    V(gg)$bte = betweenness(gg, directed = T)  #计算中介中心度 ,directed F表示无向
    png("outputs/net_betweenness.png", width = 500, height = 500)
    plot(V(gg)$bte)
    dev.off()
    
    #网络密度
    graph.density(gg)
    
    
    #（3）绘制单色球体引文网络图
     
    V(gg)$size = 1 #设置画图时每个点的大小，默认为1
    V(gg)[dte>9]$size = 3   #如果入度中心度大于9，设置大小为3
    V(gg)$label=NA          #默认不打标签
    #V(gg)$label=v(gg)$name
    V(gg)[dte>9]$label=V(gg)[dte>9]$name   #如果入度中心度大于9则打上点的名称
    V(gg)$cex=1                            #默认字体大小为1
    V(gg)[dte>9]$cex=1.2                  #如果入度中心度大于9设置字体1.2
    V(gg)$sg = com$membership + 1          #分群数目
    V(gg)$color = rainbow(max(V(gg)$sg))[V(gg)$sg]   #根据分群数设置颜色
     
    #jpeg(file = "outputs/citationnetwork.jpeg", height=768,width=1366, res=100) #输出引文图
    tiff(file="outputs/citationnetwork.tif",compression="lzw",units="in",res=600,pointsize=8,height=7.68*0.6, width=13.66*0.6)
    
    par(mar = c(0, 0, 0, 0) )
    set.seed(14)
    plot(gg, layout=layout.sphere, vertex.size = V(gg)$size,
         vertex.color = V(gg)$color, vertex.label = V(gg)$label,
         vertex.label.cex=V(gg)$cex ,vertex.label.color="blue",vertex.label.dist=1, edge.width=0.05,   edge.color = "light gray",
         edge.arrow.size=0.1)
    dev.off()
    
     
    
    
    #(4)生成重点论文条目
    
    # 文章DTE及BTE计算
    centralpapers<-data.frame(bh=as.integer(V(gg)$name),dte=V(gg)$dte,bte=V(gg)$bte)  #转成dataframe结构，编号，度中心度，中介中心度
    centralpapers<-centralpapers[order(centralpapers$bh),] #文章按照编号排序
    names(centralpapers)<-c('NEWID',"DTE",'BTE')           #列命名
    centralresult<-merge(centralpapers,articles,by.x="NEWID", by.y = "newid" ,all.x = TRUE) #同airticle数据汇总，补充发表日期的信息
    write.table(centralresult, file = "outputs/papersinfo.csv",    col.names = TRUE,row.names = FALSE ) #导出结果
    write.xlsx2(centralresult,"outputs/papersinfo.xlsx")  
     
    # 筛选重要文章，AADC>1 or AABC>5
    crucialpapers<-na.omit(centralresult[centralresult$DTE/(2020-as.integer(centralresult$py))>1  |centralresult$BTE/(2020-as.integer(centralresult$py))>5,])
    write.xlsx2(crucialpapers,"outputs/crucialpapers.xlsx")  
    
   # tcrucial<- read.xlsx2("outputs/crucialpapers.xlsx",sheetIndex = 1)
   
    
    
  #  tsql<-  " select refid,ref, cite_times  from (SELECT  refid ,trim(upper(ref)) ref,count(*)  cite_times FROM cocidetail group by refid, ref order by refid)"  
   # trefs<-data.frame( dbGetQuery(con,tsql))
   # rk<-rank(-(trefs$cite_times), ties.method = "min" )
   # trefs<-cbind(trefs,rk)
    
   # refnames<- trefs[trefs$rk<=61,]
   # refnames<- refnames[order(refnames$refid), ] #refid 排序
    
    
    
  #  tcrucial2<-tcrucial[tcrucial$NEWID %in% refnames$refid,]
    
    
    
     
   
    #重要文章的期刊分布
    crucialspread<-crucialpapers %>% group_by(so)%>%    summarise(count = n()) %>% .[order(.$count,decreasing = T),] #查看重要paper的期刊分布情况
    
   
    #(5) 重点论文类型分析  这一步先需要人工对这些文章分析
    # 重点文章类型分布图##
    ttype=c( 'Quantitative Studies','Conceptual','Modelling','Qualitative Studies','Literature Review')
    no=c( 23,14,11,6,3)
    par(mar = c(2, 2, 0, 0))
   # jpeg(file = "outputs/papertype.jpeg", height=768,width=1366, res=100)
    
    tiff(file="outputs/papertype.tif",compression="lzw",units="in",res=600,pointsize=8,height=7.68*0.6, width=13.66*0.6)
    
    
    plt <- barplot(no, col='lightblue', xaxt="n",ylim = c(0, 30))
    text(plt, par("usr")[3], labels = ttype, srt =0,adj = c(0.5,1) , xpd = TRUE, cex=1.2) 
    text(plt, no+2,no,cex=1.5)
    dev.off()
    
    
###第十部分，共被引文献初步筛选
    kk<-na.omit(centralresult[centralresult$DTE>5,]) #共筛选出96篇
    write.xlsx2(kk,"outputs/coci-papers1.xlsx")  
    kk %>% group_by(so)%>%    summarise(count = n()) %>% .[order(.$count,decreasing = T),] #查看期刊分布情况
    
    

###第十一部分，重新检索，并构建共被引数据
    #(1）清临时表
    dbRemoveTable(con,'cocidataraw')
    rawsql<- paste0("create table  cocidataraw", " (id integer, content varchar(3800), times int, source varchar(20),  addtime datetime default CURRENT_TIMESTAMP) "    )
    dbExecute (con,rawsql)
    
    
    dbExecute (con,"create index  cocidataraw_idx  on cocidataraw(id,content,times)")  #论文表
    
    
    #(2) 导入数据到临时表
    cdirnames = list.dirs(path="./cocitationdata",full.names=TRUE)
  
    filenames=list.files(path=cdirnames,pattern="*.txt",include.dirs = FALSE)#取出单个目录下的数据文件
    filenames<-str_sort(filenames, numeric = TRUE,decreasing = FALSE)
    sdirname<-substring(cdirnames,3,nchar(cdirnames))
    ttimes<-0
    for( filename in filenames){ #每个数据文件循环
      
      fullname<-paste0(pathname,"/",sdirname,"/",filename)
      print(paste("正在执行",fullname,sep=" "))
   
      #组合导入数据
      tfile<-  readLines(fullname,encoding = "UTF-8",warn=FALSE)
      tnumber<- seq(1:length(tfile))
      ttimes<-ttimes+1
      tsource<-filename
      taddtime<-Sys.time()
      tdata<- data.frame(id=tnumber,content=tfile,times=ttimes,source=tsource,addtime=taddtime)
      dbWriteTable(con, "cocidataraw",tdata,append=TRUE)
      
    }
    
    
    
    #(3) 把rawdata整理称为paper系列宽表
    dbRemoveTable(con,'cocipapers')
    prosql<- paste0("create table  cocipapers", " (ti varchar(2000),au varchar(6000),so varchar(600),py varchar(20), ab text,de varchar(1000),id int, DOI varchar(300), UTI varchar(300),CIID varchar(20)) "    )
    dbExecute (con,prosql)  #论文表
    dbExecute (con,"create index idx_cocipapers on cocipapers(id, ti,py)")  #论文表
    
    
    
    v_counter<- 0
    loopsql<- "SELECT  id  as nowid, content, substr(content,1,2) as linetype ,times as ttt, source FROM  cocidataraw    ORDER  BY times , nowid"
    totalsets<-dbGetQuery (con,loopsql)
    
    for( i in 1:length(totalsets$nowid)){ #每条记录循环处理
      if(i%%1000 ==0) print(paste0(i,Sys.time()))
      tset<-totalsets[i,]
      nowid<-tset$nowid
      content<-tset$content
      linetype<-tset$linetype
      ttt<-tset$ttt
      source<-tset$source
      
      if( tset$linetype=='PT'){   #标识新文章
        v_counter = v_counter+1
        dbExecute (con,paste0("insert into cocipapers(id,ciid) values(",v_counter, ", '",source,"')" ) )
      }
      
      
      else if( tset$linetype=='TI'){  #标题
        content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
        tisql<-paste0( "select id nid, content, substr(content,1,2) linetype from cocidataraw where substr(content,1,2)='  ' and id > ",nowid ,
                       "  and id < (select min(id) from cocidataraw where id>", nowid," and substr(content,1,2)!='  ' and times=", ttt,")",
                       " and times=",ttt)
        tiadds<-dbGetQuery(con, tisql)
        
        for( j in 1:length(tiadds$nid))
        {
          if(! is.null(tiadds)){ content<-paste(content,trimws(tiadds[j,"content"]),sep = " ")}
        }
        content<- gsub("'","" , trimws( content))  
        dbExecute(con,paste0("update cocipapers set ti= '",content," ' where id=",v_counter))
        
      }
      
      
      else if( tset$linetype=='AU'){  #作者
        content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
        ausql<-paste0( "select id nid, content, substr(content,1,2) linetype from cocidataraw where substr(content,1,2)='  ' and id > ",nowid ,
                       "  and id < (select min(id) from cocidataraw where id>", nowid," and substr(content,1,2)!='  ' and times=", ttt,")",
                       " and times=",ttt)
        auadds<-dbGetQuery(con, ausql)
        
        for( j in length(auadds$nid))
        {
          if(! is.null(auadds)){content<-paste(content,trimws(auadds[j,"content"]),sep = "; ")}
        }
        content<- gsub("'","" , trimws( content))  
        dbExecute(con,paste0("update cocipapers set au= '",content," ' where id=",v_counter))
        
      }
      
      
      else if( tset$linetype=='SO'){  #期刊名
        content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
        sosql<-paste0( "select id nid, content, substr(content,1,2) linetype from cocidataraw where substr(content,1,2)='  ' and id > ",nowid ,
                       "  and id < (select min(id) from cocidataraw where id>", nowid," and substr(content,1,2)!='  ' and times=", ttt,")",
                       " and times=",ttt)
        soadds<-dbGetQuery(con, sosql)
        
        for( j in length(soadds$nid))
        {
          if(! is.null(soadds)){content<-paste(content,trimws(soadds[j,"content"]),sep = " ") }
        }
        content<- gsub("'","" , trimws( content))  
        dbExecute(con,paste0("update cocipapers set so= '",content," ' where id=",v_counter))
        
      }
      
      
      else if( tset$linetype=='PY'){  #出版日期
        content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
        dbExecute(con,paste0("update cocipapers set py= '",content," ' where id=",v_counter))
      }
      
      
      else if( tset$linetype=='DI'){  #doi号
        content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
        dbExecute(con,paste0("update cocipapers set DOI= '",content,"' where id=",v_counter))
      }  
      
      
      else if( tset$linetype=='UT'){  #doi号
        content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
        dbExecute(con,paste0("update cocipapers set UTI= '",content," ' where id=",v_counter))
      }  
      
      
      else if( tset$linetype=='AB'){  #摘要
        content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
        dbExecute(con,paste0("update cocipapers set ab= '",content," ' where id=",v_counter))
      }
      
      
      else if( tset$linetype=='DE'){  #关键字
        content<- gsub("'","" , trimws(str_sub(content,3,nchar(content)) ))
        desql<-paste0( "select id nid, content, substr(content,1,2) linetype from cocidataraw where substr(content,1,2)='  ' and id > ",nowid ,
                       "  and id < (select min(id) from cocidataraw where id>", nowid," and substr(content,1,2)!='  ' and times=", ttt,")",
                       " and times=",ttt)
        deadds<-dbGetQuery(con, desql)
        
        for( j in length(deadds$nid))
        {
          if(! is.null(deadds)){ content<-paste(content,trimws(deadds[j,"content"]),sep = " ")}
        }
        content<- gsub("'","" , trimws( content))  
        dbExecute(con,paste0("update cocipapers set de= '",content," ' where id=",v_counter))
        
      }
      
    }
    
    #(4) 去除无效字符
    tsql <- "update cocipapers  set ciid= replace(replace(replace(replace( replace (ciid,'.txt','') ,'A',''),'B',''),'C',''),'D','') "
    dbExecute(con,tsql)
    
    
    #(5) ncocipapers 增加中间处理步骤,处理CIID为数值类型NID 
    dbRemoveTable(con,"ncocipapers")   #存放临时表信息
    #施引论文标题     施引论文作者     施引论文期刊    施引论文发表日期   施引论文摘要                           施引论文DOI      被引论文 ID
    dbExecute(con," create table ncocipapers(ti varchar(2000),au varchar(6000),so varchar(600),py varchar(20), ab clob,de varchar(1000),id int,  UTI varchar(200),DOI varchar(300),CIID varchar(20),NID integer)  ")
    dbExecute(con,"insert into ncocipapers( TI, UTI,DOI, CIID) select  ti, UTI,DOI,CIID from  cocipapers   ")
    dbExecute(con,"update ncocipapers set NID= cast(CIID as integer)")
    
     
    
    #(6) 最终组合目标文献的被引信息  共被引分析是对下载的初始文献进行共被引，而非对所含引文分析
    dbRemoveTable(con,"cocidetail");#存放临时表信息
                                           #施引论文标题      施引论文SCI号        施引论文DOI       被引论文ID     被引论文DOI        被引论文信息
    dbExecute(con,"create table  cocidetail ( TI varchar(800),UTI varchar(200),DOI varchar(200), REFID integer,REFDOI varchar(200),REF varchar(800)  ,addtime datetime default CURRENT_TIMESTAMP   )  ");
    dbExecute(con,"insert into cocidetail( TI,UTI,DOI,REFID,REFDOI) select  upper(a.ti),a.UTI, upper(a.DOI), a.NID, upper(b.DOI) from  ncocipapers a left join  is_select_final b on a.NID=b.NEWID")
    
    
    # (7) 补充被引文的REF信息，这个信息仅做参考用
 
    dbRemoveTable(con,"tref");#存放临时表信息
    dbExecute(con,"create table  tref ( REF varchar(800), REFDOI varchar(200) ,addtime datetime DEFAULT CURRENT_TIMESTAMP   )  ");
    dbExecute(con,"  create index idx_tref on   tref (REFDOI    ) ")
    getref<-paste0("insert into tref(ref, refdoi) select   distinct  ref   , replace( replace(substr(ref ,  instr(ref,  'DOI ' )+4 , length(ref) -instr(ref,  'DOI ' )-3 ) ,'[','') ,']','') ",
                  " from is_cite_all_final where  instr(ref,  'DOI ' )  >0 ")
    dbExecute(con,getref)
    dbExecute(con,"update tref set refdoi=upper(refdoi)")
    
    addref<- "update  cocidetail   set  ref = (select ref from  tref  where cocidetail.refdoi = tref.refdoi ) "
    dbExecute(con,addref)
    
     
###第十二部分，生成共被引矩阵，并结合spss分析
    
    setwd('D:/projects/Rs/cogem/cociresult')
    #用于取到底引用排前多少篇
    size =100
    
    todaydir<- as.character( Sys.Date())
    todaydir
    if(dir.exists(todaydir))
    {
      unlink(todaydir,recursive =TRUE)
      
    }
    dir.create(todaydir)
    setwd(todaydir)
    
    ccm<-NULL
    getwd()
   
    for(i in 1:70){
      
      #建立以size變數為名的資料夾
      dir.create(paste0("./", size))
      #更改工作目錄到新建立的資料夾
      setwd(paste0("./", size))   
      
      #依照被引用次數，取出被引用次數前size名的文章
      tsql<-  " select refid,ref, cite_times  from (SELECT  refid ,trim(upper(ref)) ref,count(*)  cite_times FROM cocidetail group by refid, ref order by refid)"  
      trefs<-data.frame( dbGetQuery(con,tsql))
      rk<-rank(-(trefs$cite_times), ties.method = "min" )
      trefs<-cbind(trefs,rk)
      
      refnames<- trefs[trefs$rk<=size,]
      refnames<- refnames[order(refnames$refid), ] #refid 排序
      write.xlsx(refnames, "high-cite_article.xlsx")
      #################
       
      
      #构建引文矩阵
      
      msize= dim(refnames)[1]  #矩阵大小
      ccmnames<- refnames$refid
      ccm<- matrix(data=0,  #初始化  全零
                   nrow =  msize, # 行取文章篇数
                   ncol = msize,  #  列取文章篇数
                   byrow = FALSE, dimnames = list(ccmnames,ccmnames))
      
      for(i in 1:msize)  
      {
        for(j in 1:msize)
        {
          if(i<=j){                      
               ijsql<- paste0('select count(*) num from (SELECT    uti, count(*)  FROM    cocidetail where refid in ( ', 
                           refnames[i,1], ',',refnames[j,1] ,') group by uti having count(*) >1)') #分组统计UTI号和如果大于1，说明存在共被引
               nval<-dbGetQuery(con,ijsql)
               print(paste0('doing ',i,'lun',j,'ci'))
               ccm[i,j]=nval$num
               ccm[j,i]=nval$num
            }
         }
        
      }
      
       
      
      #將矩陣的對角線以每一列中的最大值取代
      for( i in 1:nrow(ccm)){
        ccm[ i, i ] = max(ccm[ i, -i ])+1##取除了对角线外最大的值，也就是同一篇文献的话取个除了这种情况外的最大值即可，否则每篇文章出现的次数失真
      }
      #輸出最後的cocitation matrix
      write.csv(ccm, paste0("co-citation_matrix_", size, ".csv"))
      write.xlsx(ccm, "co-citation_matrix.xlsx", col.names = FALSE, row.names = FALSE)
      
      #將輸出後的cocitation matrix讀入RStudio
      ccm_data <- read.csv(file = paste0("co-citation_matrix_", size, ".csv"), header=TRUE)
      #去除表格中的第一欄
      write.xlsx(ccm_data, paste0('show_', size, '.xlsx'), row.names = FALSE)
      
      size = size - 1
      #把工作目錄往上一層移動
      setwd("../")
     
    }
    
   
    
###第十三部分，共被引文献基本展示
    
    #(1)共被引论文期刊分布
    
    #nsql<-  " select refid,ref, cite_times,rank() over (order by cite_times desc) as rk from (SELECT  refid ,trim(upper(ref)) ref,count(*)  cite_times FROM cocidetail group by refid, ref order by refid)"  
    #trefs<-data.frame(dbGetQuery(con,nsql))
    #refnames<- trefs[trefs$rk<=61,]
    #refnames<- refnames[order(refnames$refid), ] #refid 排序
    
    aa<-dbGetQuery(con,"select * from cocidetail")
    bb<-dbGetQuery(con,"select * from is_select_final")
    
    tsql<-  " select a.refid,a.ref, a.cite_times, b.so  from (SELECT  refid ,trim(upper(ref)) ref,count(*)  cite_times FROM cocidetail group by refid, ref order by refid) a , is_select_final b where a.refid=b.newid"  
    trefs<-data.frame( dbGetQuery(con,tsql))
    rk<-rank(-(trefs$cite_times), ties.method = "min" )
    trefs<-cbind(trefs,rk)
    refnames<- trefs[trefs$rk<=61,]
    refnames<- refnames[order(refnames$refid), ] #refid 排序
    
    sjournals<-data.frame(refnames$so)
    names(sjournals)<- c ('name' )
    
    
    sos1<- as.data.frame(table(sjournals) )
    sos2 <-  sos1[ order(sos1$Freq, decreasing = TRUE),]
    
    
    names(sos2)<- c ('name','number')
    par(mar = c(2, 2, 0, 0))
   # jpeg(file = "outputs/cocitation_journal.jpeg", height=650,width=1366, res=100)
    tiff(file="outputs/cocitation_journal.jpeg.tif",compression="lzw",units="in",res=600,pointsize=8,height=7.68*0.6, width=13.66*0.6)
    
    plt<- barplot(rev(sos2$number),horiz=T,xlim=c(-35,20),axes='F',col='lightblue' )
    text(plt,x=-21,label=rev(sos2$name))
    text(plt,x=-5,label=rev(sos2$number))
    axis(3,c(0,5,10,15),c(0,5,10,15))
     
    dev.off()
    
    
   dbDisconnect(con)
    
    