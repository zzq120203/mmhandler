1. create_inode

        "local t = redis.call('incr', 'INO_CUR'); if t then local t0 = t; t = t..\",\"..ARGV[2]; local v = redis.call('hget', KEYS[1]..t0, ARGV[3]); if v == false then v = '0' end; if v == ARGV[4] then redis.call('hset', KEYS[1]..t0, ARGV[1], t); return t; else return ARGV[2] end; else return ARGV[2] end",

2. create_dentry

3. delete_dentry

4. update_inode

5. update_sb

6. update_block

   local r = {};
   local x = redis.call('hexists', KEYS[1], ARGV[1]);
   local y = nil;
   if x == 1 then
      y=redis.call('hget', KEYS[1], ARGV[1]);
      if y == ARGV[2] then
         return {2,y};
      end;
   end;
   x=redis.call('hset', KEYS[1], ARGV[1], ARGV[2]);
   return {x,y};

7. clear_block

   local r = {};
   local x = redis.call('hexists', KEYS[1], ARGV[1]); 
   if x == 1 then 
      local y=redis.call('hget', KEYS[1], ARGV[1]);
      redis.call('hdel', KEYS[1], ARGV[1]);
      r = {1, y};
   else
      r = {0};
   end;
   return r;

8. dup_detect

   local x = redis.call('hexists', KEYS[1], ARGV[1]);
   if x == 1 then
      redis.call('hincrby', '_DUPSET_', KEYS[1]..'@'..ARGV[1], 1);
      return redis.call('hget', KEYS[1], ARGV[1]);
   else
      return nil;
   end

9. dedup

   local x = redis.call('hincrby', '_DUPSET_', KEYS[1]..'@'..ARGV[1], -1);
   if x < 0 then
      redis.call('hdel', '_DUPSET_', KEYS[1]..'@'..ARGV[1]);
      x=redis.call('hdel', KEYS[1], ARGV[1]);
   else
      x=-(x+1);
   end;
   return x;