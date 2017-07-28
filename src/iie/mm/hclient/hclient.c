#define CURL_STATICLIB  //必须在包含curl.h前定义

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>
#include <curl/easy.h>
#include "hiredis.h"

#define MAX_BUF  6553600

char wr_buf[MAX_BUF+1]; 
int  wr_index; 

int init(char *url);
int libcurlget(char *server, char *key, void **buffer, size_t *len);

static redisReply* reply;
      
/************************************************************************/
/* 功能: 初始化,此处的url为服务器的主机名和端口号，形式为主机名：端口号（例 localhost:8080），
		url获取途径是通过解析数据库DB的properties属性，properties属性的key的值是mm.url，
		通过此key的值获取value，即url。
   实现过程：
   		1，connect to redis
		2，得到string数组  保存的httpservice
		3，访问地址
    返回值：
*/
/************************************************************************/    
int init(char *url)
{
    //根据url切割到所需要的IP和PORT，用strtok函数分割字符串
    char *token=strtok(url,":");
	int i = 1;
	char *ip,*port;
	int iport;

    while (token!=NULL) {
		if(i == 1)
			ip = token;
       	if(i == 2)
        	port = token;
        token=strtok(NULL,":");
        if(token != NULL)
        	i++;
    }
	iport = atoi(port);
	//根据得到的ip和port，连接redis

    //以带有超时的方式链接Redis服务器，同时获取与Redis连接的上下文对象。  
    //该对象将用于其后所有与Redis操作的函数。
    redisContext* c = redisConnect(ip, iport);
	if (c->err) {  
		redisFree(c);  
		return -1;
	} 
	//从redis获得需要的内容
	const char* command = "ZRANGE mm.active.http 0 -1"; 
	reply = (redisReply*)redisCommand(c,command);
	//需要注意的是，如果返回的对象是NULL，则表示客户端和服务器之间出现严重错误，必须重新链接。 
	if (NULL == reply) {  
          redisFree(c);  
         return -1;
    }  
    //不同的Redis命令返回的数据类型不同，在获取之前需要先判断它的实际类型。
    //字符串类型的set命令的返回值的类型是REDIS_REPLY_STATUS，REDIS_REPLY_ARRAY命令返回一个数组对象。
	/*
	if (!(reply->type == REDIS_REPLY_STATUS && strcasecmp(reply->str,"OK") == 0)) {  
         printf("Failed to execute command[%s].\n",command);  
         freeReplyObject(reply);  
         redisFree(c);  
         return;  
    }  
    */
    
    if ( reply->type == REDIS_REPLY_ERROR )  
        printf( "Error: %s\n", reply->str );  
    else if ( reply->type != REDIS_REPLY_ARRAY )  
        printf( "Unexpected type: %d\n", reply->type );  
    else {  
        for ( i=0; i<reply->elements; ++i ){  
        	printf( "Result:%d: %s\n", i, reply->element[i]->str );  
        }  
    }  
	printf( "Total Number of Results: %d\n", i ); //测试
	printf( "Total Server of Results ( only one ): %s\n", reply->element[0]->str ); 
    printf("Succeed to execute command[%s].\n",command); 

	//由于后面重复使用该变量，所以需要提前释放，否则内存泄漏。  
    //freeReplyObject(reply);

    CURLcode code = curl_global_init(CURL_GLOBAL_ALL);

    if (code != CURLE_OK) {
        printf("CURL global init failed w/ %d\n", code);
        err = CURLE_OK;
        goto out;
    }

out:
	return err;
}  
  
/************************************************************************/
/* 功能: 同步的存储一个多媒体对象，并返回其存储元信息
   参数: 此处的key是对应多媒体内容的集合和“键”（形如set:md5）形成的字符串，content为多媒体的内容组成的字节数组
*/
/************************************************************************/      
char *put(char *key, void *content, size_t len)
{
    return NULL;
}

/************************************************************************/
/* 功能: 异步的存储从redis获取LHOST,LPORT一个多媒体对象，不返回任何信息
   参数: 此处的key是对应多媒体内容的集合和“键”（形如set:md5）形成的字符串，content为多媒体的内容组成的字节数组
*/
/************************************************************************/ 
char *iput(char *key, void *content, size_t len)
{
    return NULL;
}


/************************************************************************/
/* 功能: 同步批量的存储一个多媒体对象，并返回其存储元信息
   参数: 此处的key是对应多媒体内容的集合和“键”（形如set:md5）形成的字符串，content为多媒体的内容组成的字节数组
*/
/************************************************************************/  
char *mput(char **key, void **content, size_t len, int keynr)
{
    return NULL;
}

/************************************************************************/
/* 功能: 同步的对单个多媒体对象进行读取，通过接收混合的key（可以是set:key(形如set:md5)，或者是索引信息），
		返回由单个多媒体内容组成的字节数组
   参数:
*/
/************************************************************************/  
int get(char *key, void **buffer, size_t *len)
{
    struct MemoryStruct chunk;
    int err = -1;
    
    if (reply) {
    }

    return err;
}

/************************************************************************/
/* 功能: 异步的对单个多媒体对象进行读取，通过接收混合的key（可以是set:key(形如set:md5)，
		或者是索引信息），返回一个ID；
   参数:
*/
/************************************************************************/  
long iget(char *key)
{
    return -1;
}


/************************************************************************/
/* 功能: 异步批量的对多个多媒体对象读取，接受由混合的key（可以是set:key(形如set:md5)），
		或者是索引信息组成的字符串数组，返回值为由多个ID组成的set集合；
   参数:
*/
/************************************************************************/  
int imget(char **key, void **buffer, size_t *len, int keynr)
{
    return -1;
}

/************************************************************************/
/* 功能: 将imGet方法返回的由多个ID组成的set集合作为参数接入，等待服务器的处理，处理完将结果写入Map集合，
		此map的key是imGet方法中要取得的多媒体对象的key，value为由服务器返回的与key对应的多媒体对象。
   参数:
*/
/************************************************************************/  
int wait(long *ids, int nr, void **buffer, size_t *len)
{
    return -1;
}


/************************************************************************/
/* 功能: 核心功能测试版
   参数:
*/
/************************************************************************/
    
/* This can use to download images according it's url.  */

size_t write_data_test(void *buffer, size_t size, size_t nmemb, FILE *stream)
{
    size_t written;
    written = fwrite(buffer, size, nmemb, stream);
    return written;
}

/* Write data callback function*/
/*
    CURLOPT_WRITEFUNCTION //设置回调函数
    回调函数原型为: size_t function( void *buffer, size_t size, size_t nmemb, void *userp);
    函数将在libcurl接收到数据后被调用。
    void *buffer是下载回来的数据.void *userp是用户指针, 用户通过这个指针传输自己的数据.
    CURLOPT_WRITEDATA 设置回调函数中的void *userp指针的来源。
*/
size_t write_data( void *buffer, size_t size, size_t nmemb, void *userp )
{
    int segsize = size * nmemb;
    /* Check to see if this data exceeds the size of our buffer.*/
    if ( wr_index + segsize > MAX_BUF )
    {
        *(int *)userp = 1;
        return 0;
    }
    /* Copy the data from the curl buffer into our buffer */
    memcpy( (void *)&wr_buf[wr_index], buffer, (size_t)segsize );
    /* Update the write index */
    wr_index += segsize;
    /* Null terminate the buffer */
    wr_buf[wr_index] = 0;
    /* Return the number of bytes received, indicating to curl that all is okay */
    return segsize;
}

int libcurlget(char *server,char *key, void **buffer, size_t *len)
{
    CURL *curl;
    CURLcode res;
    int wr_error;
    wr_error = 0;
    wr_index = 0;

    /* 初始化libcurl */
    CURLcode code;
    char *error = "error";
    curl = curl_easy_init();
    if (!curl)
    {
        printf("couldn't init curl\n");
        return -1;
    }
    if (curl == NULL)
    {
        printf( "Failed to create curl connection\n");
        return -1;
    }
    if(curl)
    {
        char url[4096];
        char str[4096000];
        sprintf(url, "http://%s/get?key=%s", server, key);

        curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
        code = curl_easy_setopt(curl, CURLOPT_URL, url);
        if (code != CURLE_OK)
        {
            printf("Failed to set URL [%s]\n", error);
            return -1;
        }
        code = curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1);
        if (code != CURLE_OK)
        {
            printf( "Failed to set redirect option [%s]\n", error );
            return -1;
        }
    	
        /* 核心功能实现 */
        /* Tell curl the URL of the file we're going to retrieve */
        curl_easy_setopt(curl, CURLOPT_URL, url);
        /* Tell curl that we'll receive data to the function write_data, and also provide it with a context pointer for our error return. */
        curl_easy_setopt( curl, CURLOPT_WRITEDATA, (void *)&wr_error );
        curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, write_data );

        //CURLOPT_FOLLOWLOCATION 设置重定位URL，设置支持302重定向
        //将CURLOPT_VERBOSE属性设置为1，libcurl会输出通信过程中的一些细节
        curl_easy_setopt( curl, CURLOPT_VERBOSE , 1 ); 
        //如果使用的是http协议，请求头/响应头也会被输出。将CURLOPT_HEADER设为1，这些头信息将出现在消息的内容中
        curl_easy_setopt( curl, CURLOPT_HEADER , 1 ); 
		        	
        //curl_easy_setopt(curl, CURLOPT_URL, filename); //设置下载地址
        //curl_easy_setopt(curl, CURLOPT_TIMEOUT, 3);//设置超时时间
        //curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);//设置写数据的函数
        //curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_buffer);//设置写数据的函数
    	
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, str);//设置写数据的变量
    
        /* 执行下载 */
        res = curl_easy_perform(curl);
        /* 输出 */
        printf( "res = %d (write_error = %d)\n", res, wr_error );
        if ( res == 0 ) printf( "%s\n", wr_buf );
        *buffer = wr_buf;
        /* 清理内存，释放curl资源 */
        curl_easy_cleanup(curl);
   		
    }
    return 0;
}


/************************************************************************/
/* 功能: 核心功能
   参数:
*/
/************************************************************************/

struct MemoryStruct {
    char *memory;
    size_t size;
};
//添加一个全局变量
struct MemoryStruct chunk;
static void *myrealloc(void *ptr, size_t size)
{
/* There might be a realloc() out there that doesn't like reallocing
     NULL pointers, so we take care of it here */
    if(ptr)
        return realloc(ptr, size);
    else
        return malloc(size);
}
static size_t
WriteMemoryCallback(void *ptr, size_t size, size_t nmemb, void *data)
{
    size_t realsize = size * nmemb;
    //我们给定了一个足够大的内存，不需要重新申请
    struct MemoryStruct *mem = (struct MemoryStruct *)data;
    mem->memory = (char *)myrealloc(mem->memory, mem->size + realsize + 1);
    if (mem->memory) {
        memcpy(&(mem->memory[mem->size]), ptr, realsize);
        mem->size += realsize;
        mem->memory[mem->size] = 0;
    }
    return realsize;
}

//int main(int argc, char **argv)
//int getFileInBuffer(char *server,char *key,char * buffer)
int getFileInBuffer(char *key, void **buffer, size_t *len)
{
    CURL *curl_handle;
    //取消原来的注释
    //struct MemoryStruct chunk;
    //根据传递的buffer进行初始化
    chunk.memory=NULL; /* we expect realloc(NULL, size) to work */
    chunk.size = 0;    /* no data at this point */

    CURLcode code;
    char *error = "error";

    curl_handle = curl_easy_init();
    if (!curl_handle)
    {
        printf("couldn't init curl_handle\n");
        return -1;
    }
    if (curl_handle == NULL)
    {
        printf( "Failed to create curl_handle connection\n");
        return -1;
    }

    /* init the curl session */
    curl_handle = curl_easy_init();
    /* specify URL to get */
    char url[4096];
    int i = 0;
    for( i=0; i< reply->elements;i++)
    {
        sprintf(url, "http://%s/get?key=%s", reply->element[i]->str, key);
        printf( "url%d: %s\n", i,url);

        curl_easy_setopt(curl_handle, CURLOPT_URL, url);
        curl_easy_setopt(curl_handle, CURLOPT_TIMEOUT, 10);//设置超时时间，单位s
        /* send all data to this function */
        curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
        /* we pass our 'chunk' struct to the callback function */
        curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&chunk);
        /* some servers don't like requests that are made without a user-agent field, so we provide one */
        curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");
        /* 将CURLOPT_VERBOSE属性设置为1，libcurl会输出通信过程中的一些细节 */
        curl_easy_setopt( curl_handle, CURLOPT_VERBOSE , 1 ); 
        /* 如果使用的是http协议，请求头/响应头也会被输出。将CURLOPT_HEADER设为1，这些头信息将出现在消息的内容中 */
        curl_easy_setopt( curl_handle, CURLOPT_HEADER , 1 ); 
        /* get it! */
        code = curl_easy_perform(curl_handle);
        /* 判断是否成功 */
        if(CURLE_OK != code) return -1;
        /* cleanup curl stuff */
        curl_easy_cleanup(curl_handle);
        /*
        * Now, our chunk.memory points to a memory block that is chunk.size
        * bytes big and contains the remote file.
        * Do something nice with it!
        * You should be aware of the fact that at this point we might have an
        * allocated data block, and nothing has yet deallocated that data. So when
        * you're done with it, you should free() it as a nice application.
        */
        *buffer = chunk.memory;
        *len = chunk.size;
    
    	//if(chunk.memory)
        	//free(chunk.memory);
        	/* we're done with libcurl, so clean it up */
        	//curl_global_cleanup();
      		//return chunk.size;

        return 0;
    }
}



int main(void)
{
    //char url[] = "192.168.1.221:6379";
    char url[] = "192.168.1.37:6379";
    //char url[] = "127.0.0.1:6379";
	init(url);

	void *buffer;
	size_t len;
	int i = 0;
    //char key[] = "1@test@1@0@299071@44233@/mnt/data1/#1@test@1@0@0@44233@.";
    //char key[] = "test@ba3acbe8e6a52943d75bfdbd63c3ea42";//37image
    //char key[] = "1@test2@1@0@0@47941@.#1@test2@2@0@44233@47941@.";//37,38
    char key[] = "1@test2@2@0@44233@47941@.";//38image
    //for( i=0; i< reply->elements;i++)
        //libcurlget(reply->element[i]->str,key,&buffer,&len);
	
	//FILE *fp = fopen("test","wb");
	//fseek(file,0,SEEK_SET);
	//fwrite(buffer,1,len,fp);
	//fclose(fp);

    //char buffertest[1024 * 10];
    void*buf;

    int size = getFileInBuffer(key,&buf,&len);
    printf( "******************************************: %d\n", size);
    

}
