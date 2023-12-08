xquery version '3.1';
(:~
Library to manage running multiple jobs using BaseX job:eval, 
where the jobs are the execution of one function for a set of arguments

The function will have signature `fn($key as xs:string) as element()`
The function will typically create outputs and have side effects.

requires basex 10+
@licence BSD
@author: quodatum
@date: 2023/02/12
:)
module namespace wrangle = 'urn:quodatum:wrangler';
(:~ semantic version :)
declare variable $wrangle:version:="1.0.0";
(: used in bindings to indicate a wrangle job and as store key :)
declare variable $wrangle:id:="_wrangle";

(:~
submit wrangle jobs for each $item
@param wrangle data{xq:...,bindings:..}
@return unique id for the job set
:)
declare function wrangle:queue($items as item()*,$wrangle as map(*))
as xs:string{
let $wid:=random:uuid()
let $opts:=map{"cache":true()}
let $jobs:=$items!job:eval($wrangle?xq, 
                            map:merge(($wrangle?bindings(.),map:entry($wrangle:id,$wid))),
                            $opts)
                       
let $current:=store:get-or-put($wrangle:id,function(){map{}})
let $this:=map:entry($wid,map:entry("jobs",$jobs!map:entry(.,
                                                 map{
                                                    "complete":false(),
                                                    "details":job:list-details(.)
                                                 }))) 

let $_:=store:put($wrangle:id,map:merge((($this,$current))))                            
return $wid
};

(:~ active wrangle ids :)
declare function wrangle:active()
as xs:string*{
    job:list()!job:bindings(.)?($wrangle:id)=>distinct-values()
};

(:~ known wrangles :)
declare function wrangle:list()
as xs:string*{
    store:get-or-put($wrangle:id,function(){map{}})=>map:keys()
};

(:~ known wrangles :)
declare function wrangle:list-details($wid as xs:string)
as map(*){
    store:get-or-put($wrangle:id,function(){map{}})=>map:get($wid)
};

(:~ all wrangled jobs :)
declare function wrangle:job-list()
as xs:string*{
job:list()[job:bindings(.)=>map:contains($wrangle:id)]
};

(:~  jobs for wrangle id :)
declare function wrangle:job-list($wid as xs:string)
 as xs:string*{
   job:list()[job:bindings(.)?($wrangle:id) eq $wid]
};
(:~  is wrangle id finished (or unknown) :)
declare function wrangle:finished($wid as xs:string)
 as xs:string*{
   every $job in job:list()[job:bindings(.)?($wrangle:id) eq $wid] satisfies job:finished($job)
};

(:~  cancel wrangle id :)
declare function wrangle:remove($wid as xs:string)
as  empty-sequence(){
   job:list()[job:bindings(.)?($wrangle:id) eq $wid]!job:remove(.),
   store:put($wrangle:id,store:get($wrangle:id)=>map:remove($wid))
};

(:~ tally of non-zero job status for $wid  "scheduled", "queued", "running", "cached" :)
declare function wrangle:status($wid as xs:string)
as map(*){
    wrangle:job-list($wid)!job:list-details(.)/@state/string()
    =>fold-left(map{},wrangle:tally-count#2)           
};

(:~ job-results with no error as sequence:)
declare function wrangle:results($wid as xs:string)
as item()*{
    wrangle:job-list($wid)!wrangle:job-result(.)[not(?error)]?result       
};

(:~ error counts keyed on $err:code :)
declare function wrangle:errors($wid as xs:string)
as map(*){
    wrangle:job-list($wid)!wrangle:job-result(.)[?error]?result?code!string()
    =>fold-left(map{},wrangle:tally-count#2)          
};

(:~ key is $err:code values are joblists :)
declare function wrangle:jobs-by-error($wid as xs:string)
as map(*){
  (for $jobId in wrangle:job-list($wid)
    let  $result:=wrangle:job-result($jobId)[?error]
    where exists($result)
    return map:entry($result?result?code!string(),$jobId)
   )
=> map:merge( map{"duplicates":"combine"})
};

(:~ return key for job:)
declare function wrangle:job-key($jobId as xs:string)
as xs:string{
let $b:=job:bindings($jobId)
return $b?(map:keys($b)[. ne $wrangle:id])
};

(:~ return map from peek at result:)
declare function wrangle:job-result($jobId as xs:string)
as map(*){
    try{
        map{
            "error":false(),
            "result": job:result($jobId,map{"keep":true()})
        }        
    }catch *{
        map{
            "error":true(),
            "result": map{"description": $err:description,
                         "code": $err:code,
                         "line": $err:column-number,
                         "additional": $err:additional,
                         "value":$err:value
                         }
        }      
    }
};

(:~ XQuery for background service :)
declare function wrangle:service() 
as xs:string{
``[
import module namespace wrangle = 'urn:quodatum:wrangler:v1'; at "`{ static-base-uri() }`";
let $done:=wrangle:job-list()[job:finished(.)]
if(exists($done))
then let $w:=store:get($wrangle:id)
     for $job in $done
     group by $wid= job:bindings($job)?($wrangle:id)
     for $job in $job
     let $_:=store:put($wrangle:id)
]``
 
};
(:~ schedule as service :)
declare function wrangle:schedule-service()
as xs:string{
    wrangle:service()
    =>job:eval((), map { 'id':$wrangle:id, 'service':true(),'interval': 'PT1S' })
};

(:~ @return map string->count  for fold-left :)
declare %private function wrangle:tally-count($r as map(*),$this as xs:string)
as map(*){
    map:merge(
        (map:entry($this,if(map:contains($r,$this)) then $r($this)+1 else 1),$r),
        map{"duplicates":"use-first"}
        )
};
(:~ @return map string->(string*)  for fold-left :)
declare %private function wrangle:tally-list($r as map(*),$key as xs:string,$value as xs:string)
as map(*){
    map:merge(
        (map:entry($key,$value),$r),
        map{"duplicates":"combine"}
        )
};