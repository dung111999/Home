var Rf=Object.defineProperty,Lf=Object.defineProperties;var bf=Object.getOwnPropertyDescriptors;var _a=Object.getOwnPropertySymbols;var vf=Object.prototype.hasOwnProperty,Sf=Object.prototype.propertyIsEnumerable;var Ta=(e,t,n)=>t in e?Rf(e,t,{enumerable:!0,configurable:!0,writable:!0,value:n}):e[t]=n,E=(e,t)=>{for(var n in t||={})vf.call(t,n)&&Ta(e,n,t[n]);if(_a)for(var n of _a(t))Sf.call(t,n)&&Ta(e,n,t[n]);return e},$=(e,t)=>Lf(e,bf(t));var jo=null;var Fo=1,Aa=Symbol("SIGNAL");function P(e){let t=jo;return jo=e,t}function Na(){return jo}var Vo={version:0,lastCleanEpoch:0,dirty:!1,producerNode:void 0,producerLastReadVersion:void 0,producerIndexOfThis:void 0,nextProducerIndex:0,liveConsumerNode:void 0,liveConsumerIndexOfThis:void 0,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function yf(e){if(!(zo(e)&&!e.dirty)&&!(!e.dirty&&e.lastCleanEpoch===Fo)){if(!e.producerMustRecompute(e)&&!$o(e)){e.dirty=!1,e.lastCleanEpoch=Fo;return}e.producerRecomputeValue(e),e.dirty=!1,e.lastCleanEpoch=Fo}}function Bo(e){return e&&(e.nextProducerIndex=0),P(e)}function Oa(e,t){if(P(t),!(!e||e.producerNode===void 0||e.producerIndexOfThis===void 0||e.producerLastReadVersion===void 0)){if(zo(e))for(let n=e.nextProducerIndex;n<e.producerNode.length;n++)Ho(e.producerNode[n],e.producerIndexOfThis[n]);for(;e.producerNode.length>e.nextProducerIndex;)e.producerNode.pop(),e.producerLastReadVersion.pop(),e.producerIndexOfThis.pop()}}function $o(e){qo(e);for(let t=0;t<e.producerNode.length;t++){let n=e.producerNode[t],r=e.producerLastReadVersion[t];if(r!==n.version||(yf(n),r!==n.version))return!0}return!1}function Uo(e){if(qo(e),zo(e))for(let t=0;t<e.producerNode.length;t++)Ho(e.producerNode[t],e.producerIndexOfThis[t]);e.producerNode.length=e.producerLastReadVersion.length=e.producerIndexOfThis.length=0,e.liveConsumerNode&&(e.liveConsumerNode.length=e.liveConsumerIndexOfThis.length=0)}function Ho(e,t){if(Df(e),e.liveConsumerNode.length===1&&wf(e))for(let r=0;r<e.producerNode.length;r++)Ho(e.producerNode[r],e.producerIndexOfThis[r]);let n=e.liveConsumerNode.length-1;if(e.liveConsumerNode[t]=e.liveConsumerNode[n],e.liveConsumerIndexOfThis[t]=e.liveConsumerIndexOfThis[n],e.liveConsumerNode.length--,e.liveConsumerIndexOfThis.length--,t<e.liveConsumerNode.length){let r=e.liveConsumerIndexOfThis[t],o=e.liveConsumerNode[t];qo(o),o.producerIndexOfThis[r]=t}}function zo(e){return e.consumerIsAlwaysLive||(e?.liveConsumerNode?.length??0)>0}function qo(e){e.producerNode??=[],e.producerIndexOfThis??=[],e.producerLastReadVersion??=[]}function Df(e){e.liveConsumerNode??=[],e.liveConsumerIndexOfThis??=[]}function wf(e){return e.producerNode!==void 0}function Cf(){throw new Error}var If=Cf;function Pa(e){If=e}function L(e){return typeof e=="function"}function Ct(e){let n=e(r=>{Error.call(r),r.stack=new Error().stack});return n.prototype=Object.create(Error.prototype),n.prototype.constructor=n,n}var rr=Ct(e=>function(n){e(this),this.message=n?`${n.length} errors occurred during unsubscription:
${n.map((r,o)=>`${o+1}) ${r.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=n});function fn(e,t){if(e){let n=e.indexOf(t);0<=n&&e.splice(n,1)}}var U=class e{constructor(t){this.initialTeardown=t,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let t;if(!this.closed){this.closed=!0;let{_parentage:n}=this;if(n)if(this._parentage=null,Array.isArray(n))for(let i of n)i.remove(this);else n.remove(this);let{initialTeardown:r}=this;if(L(r))try{r()}catch(i){t=i instanceof rr?i.errors:[i]}let{_finalizers:o}=this;if(o){this._finalizers=null;for(let i of o)try{ka(i)}catch(s){t=t??[],s instanceof rr?t=[...t,...s.errors]:t.push(s)}}if(t)throw new rr(t)}}add(t){var n;if(t&&t!==this)if(this.closed)ka(t);else{if(t instanceof e){if(t.closed||t._hasParent(this))return;t._addParent(this)}(this._finalizers=(n=this._finalizers)!==null&&n!==void 0?n:[]).push(t)}}_hasParent(t){let{_parentage:n}=this;return n===t||Array.isArray(n)&&n.includes(t)}_addParent(t){let{_parentage:n}=this;this._parentage=Array.isArray(n)?(n.push(t),n):n?[n,t]:t}_removeParent(t){let{_parentage:n}=this;n===t?this._parentage=null:Array.isArray(n)&&fn(n,t)}remove(t){let{_finalizers:n}=this;n&&fn(n,t),t instanceof e&&t._removeParent(this)}};U.EMPTY=(()=>{let e=new U;return e.closed=!0,e})();var Go=U.EMPTY;function or(e){return e instanceof U||e&&"closed"in e&&L(e.remove)&&L(e.add)&&L(e.unsubscribe)}function ka(e){L(e)?e():e.unsubscribe()}var be={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var It={setTimeout(e,t,...n){let{delegate:r}=It;return r?.setTimeout?r.setTimeout(e,t,...n):setTimeout(e,t,...n)},clearTimeout(e){let{delegate:t}=It;return(t?.clearTimeout||clearTimeout)(e)},delegate:void 0};function ir(e){It.setTimeout(()=>{let{onUnhandledError:t}=be;if(t)t(e);else throw e})}function hn(){}var Fa=Wo("C",void 0,void 0);function ja(e){return Wo("E",void 0,e)}function Va(e){return Wo("N",e,void 0)}function Wo(e,t,n){return{kind:e,value:t,error:n}}var it=null;function xt(e){if(be.useDeprecatedSynchronousErrorHandling){let t=!it;if(t&&(it={errorThrown:!1,error:null}),e(),t){let{errorThrown:n,error:r}=it;if(it=null,n)throw r}}else e()}function Ba(e){be.useDeprecatedSynchronousErrorHandling&&it&&(it.errorThrown=!0,it.error=e)}var st=class extends U{constructor(t){super(),this.isStopped=!1,t?(this.destination=t,or(t)&&t.add(this)):this.destination=_f}static create(t,n,r){return new Mt(t,n,r)}next(t){this.isStopped?Yo(Va(t),this):this._next(t)}error(t){this.isStopped?Yo(ja(t),this):(this.isStopped=!0,this._error(t))}complete(){this.isStopped?Yo(Fa,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(t){this.destination.next(t)}_error(t){try{this.destination.error(t)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},xf=Function.prototype.bind;function Zo(e,t){return xf.call(e,t)}var Qo=class{constructor(t){this.partialObserver=t}next(t){let{partialObserver:n}=this;if(n.next)try{n.next(t)}catch(r){sr(r)}}error(t){let{partialObserver:n}=this;if(n.error)try{n.error(t)}catch(r){sr(r)}else sr(t)}complete(){let{partialObserver:t}=this;if(t.complete)try{t.complete()}catch(n){sr(n)}}},Mt=class extends st{constructor(t,n,r){super();let o;if(L(t)||!t)o={next:t??void 0,error:n??void 0,complete:r??void 0};else{let i;this&&be.useDeprecatedNextContext?(i=Object.create(t),i.unsubscribe=()=>this.unsubscribe(),o={next:t.next&&Zo(t.next,i),error:t.error&&Zo(t.error,i),complete:t.complete&&Zo(t.complete,i)}):o=t}this.destination=new Qo(o)}};function sr(e){be.useDeprecatedSynchronousErrorHandling?Ba(e):ir(e)}function Mf(e){throw e}function Yo(e,t){let{onStoppedNotification:n}=be;n&&It.setTimeout(()=>n(e,t))}var _f={closed:!0,next:hn,error:Mf,complete:hn};var _t=typeof Symbol=="function"&&Symbol.observable||"@@observable";function ie(e){return e}function Ko(...e){return Jo(e)}function Jo(e){return e.length===0?ie:e.length===1?e[0]:function(n){return e.reduce((r,o)=>o(r),n)}}var k=(()=>{class e{constructor(n){n&&(this._subscribe=n)}lift(n){let r=new e;return r.source=this,r.operator=n,r}subscribe(n,r,o){let i=Af(n)?n:new Mt(n,r,o);return xt(()=>{let{operator:s,source:a}=this;i.add(s?s.call(i,a):a?this._subscribe(i):this._trySubscribe(i))}),i}_trySubscribe(n){try{return this._subscribe(n)}catch(r){n.error(r)}}forEach(n,r){return r=$a(r),new r((o,i)=>{let s=new Mt({next:a=>{try{n(a)}catch(c){i(c),s.unsubscribe()}},error:i,complete:o});this.subscribe(s)})}_subscribe(n){var r;return(r=this.source)===null||r===void 0?void 0:r.subscribe(n)}[_t](){return this}pipe(...n){return Jo(n)(this)}toPromise(n){return n=$a(n),new n((r,o)=>{let i;this.subscribe(s=>i=s,s=>o(s),()=>r(i))})}}return e.create=t=>new e(t),e})();function $a(e){var t;return(t=e??be.Promise)!==null&&t!==void 0?t:Promise}function Tf(e){return e&&L(e.next)&&L(e.error)&&L(e.complete)}function Af(e){return e&&e instanceof st||Tf(e)&&or(e)}function Xo(e){return L(e?.lift)}function M(e){return t=>{if(Xo(t))return t.lift(function(n){try{return e(n,this)}catch(r){this.error(r)}});throw new TypeError("Unable to lift unknown Observable type")}}function _(e,t,n,r,o){return new ei(e,t,n,r,o)}var ei=class extends st{constructor(t,n,r,o,i,s){super(t),this.onFinalize=i,this.shouldUnsubscribe=s,this._next=n?function(a){try{n(a)}catch(c){t.error(c)}}:super._next,this._error=o?function(a){try{o(a)}catch(c){t.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=r?function(){try{r()}catch(a){t.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var t;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:n}=this;super.unsubscribe(),!n&&((t=this.onFinalize)===null||t===void 0||t.call(this))}}};function Tt(){return M((e,t)=>{let n=null;e._refCount++;let r=_(t,void 0,void 0,void 0,()=>{if(!e||e._refCount<=0||0<--e._refCount){n=null;return}let o=e._connection,i=n;n=null,o&&(!i||o===i)&&o.unsubscribe(),t.unsubscribe()});e.subscribe(r),r.closed||(n=e.connect())})}var At=class extends k{constructor(t,n){super(),this.source=t,this.subjectFactory=n,this._subject=null,this._refCount=0,this._connection=null,Xo(t)&&(this.lift=t.lift)}_subscribe(t){return this.getSubject().subscribe(t)}getSubject(){let t=this._subject;return(!t||t.isStopped)&&(this._subject=this.subjectFactory()),this._subject}_teardown(){this._refCount=0;let{_connection:t}=this;this._subject=this._connection=null,t?.unsubscribe()}connect(){let t=this._connection;if(!t){t=this._connection=new U;let n=this.getSubject();t.add(this.source.subscribe(_(n,void 0,()=>{this._teardown(),n.complete()},r=>{this._teardown(),n.error(r)},()=>this._teardown()))),t.closed&&(this._connection=null,t=U.EMPTY)}return t}refCount(){return Tt()(this)}};var Ua=Ct(e=>function(){e(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var K=(()=>{class e extends k{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let r=new ar(this,this);return r.operator=n,r}_throwIfClosed(){if(this.closed)throw new Ua}next(n){xt(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let r of this.currentObservers)r.next(n)}})}error(n){xt(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:r}=this;for(;r.length;)r.shift().error(n)}})}complete(){xt(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:r,isStopped:o,observers:i}=this;return r||o?Go:(this.currentObservers=null,i.push(n),new U(()=>{this.currentObservers=null,fn(i,n)}))}_checkFinalizedStatuses(n){let{hasError:r,thrownError:o,isStopped:i}=this;r?n.error(o):i&&n.complete()}asObservable(){let n=new k;return n.source=this,n}}return e.create=(t,n)=>new ar(t,n),e})(),ar=class extends K{constructor(t,n){super(),this.destination=t,this.source=n}next(t){var n,r;(r=(n=this.destination)===null||n===void 0?void 0:n.next)===null||r===void 0||r.call(n,t)}error(t){var n,r;(r=(n=this.destination)===null||n===void 0?void 0:n.error)===null||r===void 0||r.call(n,t)}complete(){var t,n;(n=(t=this.destination)===null||t===void 0?void 0:t.complete)===null||n===void 0||n.call(t)}_subscribe(t){var n,r;return(r=(n=this.source)===null||n===void 0?void 0:n.subscribe(t))!==null&&r!==void 0?r:Go}};var Z=class extends K{constructor(t){super(),this._value=t}get value(){return this.getValue()}_subscribe(t){let n=super._subscribe(t);return!n.closed&&t.next(this._value),n}getValue(){let{hasError:t,thrownError:n,_value:r}=this;if(t)throw n;return this._throwIfClosed(),r}next(t){super.next(this._value=t)}};var se=new k(e=>e.complete());function Ha(e){return e&&L(e.schedule)}function za(e){return e[e.length-1]}function qa(e){return L(za(e))?e.pop():void 0}function Ge(e){return Ha(za(e))?e.pop():void 0}function Wa(e,t,n,r){function o(i){return i instanceof n?i:new n(function(s){s(i)})}return new(n||(n=Promise))(function(i,s){function a(l){try{u(r.next(l))}catch(d){s(d)}}function c(l){try{u(r.throw(l))}catch(d){s(d)}}function u(l){l.done?i(l.value):o(l.value).then(a,c)}u((r=r.apply(e,t||[])).next())})}function Ga(e){var t=typeof Symbol=="function"&&Symbol.iterator,n=t&&e[t],r=0;if(n)return n.call(e);if(e&&typeof e.length=="number")return{next:function(){return e&&r>=e.length&&(e=void 0),{value:e&&e[r++],done:!e}}};throw new TypeError(t?"Object is not iterable.":"Symbol.iterator is not defined.")}function at(e){return this instanceof at?(this.v=e,this):new at(e)}function Za(e,t,n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var r=n.apply(e,t||[]),o,i=[];return o=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),o[Symbol.asyncIterator]=function(){return this},o;function s(f){return function(m){return Promise.resolve(m).then(f,d)}}function a(f,m){r[f]&&(o[f]=function(w){return new Promise(function(V,B){i.push([f,w,V,B])>1||c(f,w)})},m&&(o[f]=m(o[f])))}function c(f,m){try{u(r[f](m))}catch(w){p(i[0][3],w)}}function u(f){f.value instanceof at?Promise.resolve(f.value.v).then(l,d):p(i[0][2],f)}function l(f){c("next",f)}function d(f){c("throw",f)}function p(f,m){f(m),i.shift(),i.length&&c(i[0][0],i[0][1])}}function Ya(e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var t=e[Symbol.asyncIterator],n;return t?t.call(e):(e=typeof Ga=="function"?Ga(e):e[Symbol.iterator](),n={},r("next"),r("throw"),r("return"),n[Symbol.asyncIterator]=function(){return this},n);function r(i){n[i]=e[i]&&function(s){return new Promise(function(a,c){s=e[i](s),o(a,c,s.done,s.value)})}}function o(i,s,a,c){Promise.resolve(c).then(function(u){i({value:u,done:a})},s)}}var cr=e=>e&&typeof e.length=="number"&&typeof e!="function";function ur(e){return L(e?.then)}function lr(e){return L(e[_t])}function dr(e){return Symbol.asyncIterator&&L(e?.[Symbol.asyncIterator])}function fr(e){return new TypeError(`You provided ${e!==null&&typeof e=="object"?"an invalid object":`'${e}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function Nf(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var hr=Nf();function pr(e){return L(e?.[hr])}function Er(e){return Za(this,arguments,function*(){let n=e.getReader();try{for(;;){let{value:r,done:o}=yield at(n.read());if(o)return yield at(void 0);yield yield at(r)}}finally{n.releaseLock()}})}function mr(e){return L(e?.getReader)}function G(e){if(e instanceof k)return e;if(e!=null){if(lr(e))return Of(e);if(cr(e))return Pf(e);if(ur(e))return kf(e);if(dr(e))return Qa(e);if(pr(e))return Ff(e);if(mr(e))return jf(e)}throw fr(e)}function Of(e){return new k(t=>{let n=e[_t]();if(L(n.subscribe))return n.subscribe(t);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function Pf(e){return new k(t=>{for(let n=0;n<e.length&&!t.closed;n++)t.next(e[n]);t.complete()})}function kf(e){return new k(t=>{e.then(n=>{t.closed||(t.next(n),t.complete())},n=>t.error(n)).then(null,ir)})}function Ff(e){return new k(t=>{for(let n of e)if(t.next(n),t.closed)return;t.complete()})}function Qa(e){return new k(t=>{Vf(e,t).catch(n=>t.error(n))})}function jf(e){return Qa(Er(e))}function Vf(e,t){var n,r,o,i;return Wa(this,void 0,void 0,function*(){try{for(n=Ya(e);r=yield n.next(),!r.done;){let s=r.value;if(t.next(s),t.closed)return}}catch(s){o={error:s}}finally{try{r&&!r.done&&(i=n.return)&&(yield i.call(n))}finally{if(o)throw o.error}}t.complete()})}function ne(e,t,n,r=0,o=!1){let i=t.schedule(function(){n(),o?e.add(this.schedule(null,r)):this.unsubscribe()},r);if(e.add(i),!o)return i}function gr(e,t=0){return M((n,r)=>{n.subscribe(_(r,o=>ne(r,e,()=>r.next(o),t),()=>ne(r,e,()=>r.complete(),t),o=>ne(r,e,()=>r.error(o),t)))})}function Rr(e,t=0){return M((n,r)=>{r.add(e.schedule(()=>n.subscribe(r),t))})}function Ka(e,t){return G(e).pipe(Rr(t),gr(t))}function Ja(e,t){return G(e).pipe(Rr(t),gr(t))}function Xa(e,t){return new k(n=>{let r=0;return t.schedule(function(){r===e.length?n.complete():(n.next(e[r++]),n.closed||this.schedule())})})}function ec(e,t){return new k(n=>{let r;return ne(n,t,()=>{r=e[hr](),ne(n,t,()=>{let o,i;try{({value:o,done:i}=r.next())}catch(s){n.error(s);return}i?n.complete():n.next(o)},0,!0)}),()=>L(r?.return)&&r.return()})}function Lr(e,t){if(!e)throw new Error("Iterable cannot be null");return new k(n=>{ne(n,t,()=>{let r=e[Symbol.asyncIterator]();ne(n,t,()=>{r.next().then(o=>{o.done?n.complete():n.next(o.value)})},0,!0)})})}function tc(e,t){return Lr(Er(e),t)}function nc(e,t){if(e!=null){if(lr(e))return Ka(e,t);if(cr(e))return Xa(e,t);if(ur(e))return Ja(e,t);if(dr(e))return Lr(e,t);if(pr(e))return ec(e,t);if(mr(e))return tc(e,t)}throw fr(e)}function z(e,t){return t?nc(e,t):G(e)}function v(...e){let t=Ge(e);return z(e,t)}function Nt(e,t){let n=L(e)?e:()=>e,r=o=>o.error(n());return new k(t?o=>t.schedule(r,0,o):r)}function ti(e){return!!e&&(e instanceof k||L(e.lift)&&L(e.subscribe))}var ke=Ct(e=>function(){e(this),this.name="EmptyError",this.message="no elements in sequence"});function C(e,t){return M((n,r)=>{let o=0;n.subscribe(_(r,i=>{r.next(e.call(t,i,o++))}))})}var{isArray:Bf}=Array;function $f(e,t){return Bf(t)?e(...t):e(t)}function rc(e){return C(t=>$f(e,t))}var{isArray:Uf}=Array,{getPrototypeOf:Hf,prototype:zf,keys:qf}=Object;function oc(e){if(e.length===1){let t=e[0];if(Uf(t))return{args:t,keys:null};if(Gf(t)){let n=qf(t);return{args:n.map(r=>t[r]),keys:n}}}return{args:e,keys:null}}function Gf(e){return e&&typeof e=="object"&&Hf(e)===zf}function ic(e,t){return e.reduce((n,r,o)=>(n[r]=t[o],n),{})}function br(...e){let t=Ge(e),n=qa(e),{args:r,keys:o}=oc(e);if(r.length===0)return z([],t);let i=new k(Wf(r,t,o?s=>ic(o,s):ie));return n?i.pipe(rc(n)):i}function Wf(e,t,n=ie){return r=>{sc(t,()=>{let{length:o}=e,i=new Array(o),s=o,a=o;for(let c=0;c<o;c++)sc(t,()=>{let u=z(e[c],t),l=!1;u.subscribe(_(r,d=>{i[c]=d,l||(l=!0,a--),a||r.next(n(i.slice()))},()=>{--s||r.complete()}))},r)},r)}}function sc(e,t,n){e?ne(n,e,t):t()}function ac(e,t,n,r,o,i,s,a){let c=[],u=0,l=0,d=!1,p=()=>{d&&!c.length&&!u&&t.complete()},f=w=>u<r?m(w):c.push(w),m=w=>{i&&t.next(w),u++;let V=!1;G(n(w,l++)).subscribe(_(t,B=>{o?.(B),i?f(B):t.next(B)},()=>{V=!0},void 0,()=>{if(V)try{for(u--;c.length&&u<r;){let B=c.shift();s?ne(t,s,()=>m(B)):m(B)}p()}catch(B){t.error(B)}}))};return e.subscribe(_(t,f,()=>{d=!0,p()})),()=>{a?.()}}function q(e,t,n=1/0){return L(t)?q((r,o)=>C((i,s)=>t(r,i,o,s))(G(e(r,o))),n):(typeof t=="number"&&(n=t),M((r,o)=>ac(r,o,e,n)))}function ni(e=1/0){return q(ie,e)}function cc(){return ni(1)}function Ot(...e){return cc()(z(e,Ge(e)))}function vr(e){return new k(t=>{G(e()).subscribe(t)})}function ve(e,t){return M((n,r)=>{let o=0;n.subscribe(_(r,i=>e.call(t,i,o++)&&r.next(i)))})}function We(e){return M((t,n)=>{let r=null,o=!1,i;r=t.subscribe(_(n,void 0,void 0,s=>{i=G(e(s,We(e)(t))),r?(r.unsubscribe(),r=null,i.subscribe(n)):o=!0})),o&&(r.unsubscribe(),r=null,i.subscribe(n))})}function uc(e,t,n,r,o){return(i,s)=>{let a=n,c=t,u=0;i.subscribe(_(s,l=>{let d=u++;c=a?e(c,l,d):(a=!0,l),r&&s.next(c)},o&&(()=>{a&&s.next(c),s.complete()})))}}function Pt(e,t){return L(t)?q(e,t,1):q(e,1)}function Ze(e){return M((t,n)=>{let r=!1;t.subscribe(_(n,o=>{r=!0,n.next(o)},()=>{r||n.next(e),n.complete()}))})}function Fe(e){return e<=0?()=>se:M((t,n)=>{let r=0;t.subscribe(_(n,o=>{++r<=e&&(n.next(o),e<=r&&n.complete())}))})}function ri(e){return C(()=>e)}function Sr(e=Zf){return M((t,n)=>{let r=!1;t.subscribe(_(n,o=>{r=!0,n.next(o)},()=>r?n.complete():n.error(e())))})}function Zf(){return new ke}function pn(e){return M((t,n)=>{try{t.subscribe(n)}finally{n.add(e)}})}function Ie(e,t){let n=arguments.length>=2;return r=>r.pipe(e?ve((o,i)=>e(o,i,r)):ie,Fe(1),n?Ze(t):Sr(()=>new ke))}function kt(e){return e<=0?()=>se:M((t,n)=>{let r=[];t.subscribe(_(n,o=>{r.push(o),e<r.length&&r.shift()},()=>{for(let o of r)n.next(o);n.complete()},void 0,()=>{r=null}))})}function oi(e,t){let n=arguments.length>=2;return r=>r.pipe(e?ve((o,i)=>e(o,i,r)):ie,kt(1),n?Ze(t):Sr(()=>new ke))}function ii(e,t){return M(uc(e,t,arguments.length>=2,!0))}function si(...e){let t=Ge(e);return M((n,r)=>{(t?Ot(e,n,t):Ot(e,n)).subscribe(r)})}function Se(e,t){return M((n,r)=>{let o=null,i=0,s=!1,a=()=>s&&!o&&r.complete();n.subscribe(_(r,c=>{o?.unsubscribe();let u=0,l=i++;G(e(c,l)).subscribe(o=_(r,d=>r.next(t?t(c,d,l,u++):d),()=>{o=null,a()}))},()=>{s=!0,a()}))})}function ai(e){return M((t,n)=>{G(e).subscribe(_(n,()=>n.complete(),hn)),!n.closed&&t.subscribe(n)})}function Y(e,t,n){let r=L(e)||t||n?{next:e,error:t,complete:n}:e;return r?M((o,i)=>{var s;(s=r.subscribe)===null||s===void 0||s.call(r);let a=!0;o.subscribe(_(i,c=>{var u;(u=r.next)===null||u===void 0||u.call(r,c),i.next(c)},()=>{var c;a=!1,(c=r.complete)===null||c===void 0||c.call(r),i.complete()},c=>{var u;a=!1,(u=r.error)===null||u===void 0||u.call(r,c),i.error(c)},()=>{var c,u;a&&((c=r.unsubscribe)===null||c===void 0||c.call(r)),(u=r.finalize)===null||u===void 0||u.call(r)}))}):ie}var Wc="https://g.co/ng/security#xss",g=class extends Error{constructor(t,n){super(Qi(t,n)),this.code=t}};function Qi(e,t){return`${`NG0${Math.abs(e)}`}${t?": "+t:""}`}function Ki(e){return{toString:e}.toString()}var mi=globalThis;function j(e){for(let t in e)if(e[t]===j)return t;throw Error("Could not find renamed property on target object.")}function ae(e){if(typeof e=="string")return e;if(Array.isArray(e))return"["+e.map(ae).join(", ")+"]";if(e==null)return""+e;if(e.overriddenName)return`${e.overriddenName}`;if(e.name)return`${e.name}`;let t=e.toString();if(t==null)return""+t;let n=t.indexOf(`
`);return n===-1?t:t.substring(0,n)}function lc(e,t){return e==null||e===""?t===null?"":t:t==null||t===""?e:e+" "+t}var Yf=j({__forward_ref__:j});function Zc(e){return e.__forward_ref__=Zc,e.toString=function(){return ae(this())},e}function de(e){return Yc(e)?e():e}function Yc(e){return typeof e=="function"&&e.hasOwnProperty(Yf)&&e.__forward_ref__===Zc}function b(e){return{token:e.token,providedIn:e.providedIn||null,factory:e.factory,value:void 0}}function Qr(e){return dc(e,Kc)||dc(e,Jc)}function Qc(e){return Qr(e)!==null}function dc(e,t){return e.hasOwnProperty(t)?e[t]:null}function Qf(e){let t=e&&(e[Kc]||e[Jc]);return t||null}function fc(e){return e&&(e.hasOwnProperty(hc)||e.hasOwnProperty(Kf))?e[hc]:null}var Kc=j({\u0275prov:j}),hc=j({\u0275inj:j}),Jc=j({ngInjectableDef:j}),Kf=j({ngInjectorDef:j}),y=class{constructor(t,n){this._desc=t,this.ngMetadataName="InjectionToken",this.\u0275prov=void 0,typeof n=="number"?this.__NG_ELEMENT_ID__=n:n!==void 0&&(this.\u0275prov=b({token:this,providedIn:n.providedIn||"root",factory:n.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function Xc(e){return e&&!!e.\u0275providers}var Jf=j({\u0275cmp:j}),Xf=j({\u0275dir:j}),eh=j({\u0275pipe:j}),th=j({\u0275mod:j}),_r=j({\u0275fac:j}),gn=j({__NG_ELEMENT_ID__:j}),pc=j({__NG_ENV_ID__:j});function Ji(e){return typeof e=="string"?e:e==null?"":String(e)}function nh(e){return typeof e=="function"?e.name||e.toString():typeof e=="object"&&e!=null&&typeof e.type=="function"?e.type.name||e.type.toString():Ji(e)}function rh(e,t){let n=t?`. Dependency path: ${t.join(" > ")} > ${e}`:"";throw new g(-200,e)}function Xi(e,t){throw new g(-201,!1)}var D=function(e){return e[e.Default=0]="Default",e[e.Host=1]="Host",e[e.Self=2]="Self",e[e.SkipSelf=4]="SkipSelf",e[e.Optional=8]="Optional",e}(D||{}),gi;function eu(){return gi}function le(e){let t=gi;return gi=e,t}function tu(e,t,n){let r=Qr(e);if(r&&r.providedIn=="root")return r.value===void 0?r.value=r.factory():r.value;if(n&D.Optional)return null;if(t!==void 0)return t;Xi(e,"Injector")}var oh={},Rn=oh,ih="__NG_DI_FLAG__",Tr="ngTempTokenPath",sh="ngTokenPath",ah=/\n/gm,ch="\u0275",Ec="__source",Bt;function uh(){return Bt}function Ye(e){let t=Bt;return Bt=e,t}function lh(e,t=D.Default){if(Bt===void 0)throw new g(-203,!1);return Bt===null?tu(e,void 0,t):Bt.get(e,t&D.Optional?null:void 0,t)}function x(e,t=D.Default){return(eu()||lh)(de(e),t)}function h(e,t=D.Default){return x(e,Kr(t))}function Kr(e){return typeof e>"u"||typeof e=="number"?e:0|(e.optional&&8)|(e.host&&1)|(e.self&&2)|(e.skipSelf&&4)}function Ri(e){let t=[];for(let n=0;n<e.length;n++){let r=de(e[n]);if(Array.isArray(r)){if(r.length===0)throw new g(900,!1);let o,i=D.Default;for(let s=0;s<r.length;s++){let a=r[s],c=dh(a);typeof c=="number"?c===-1?o=a.token:i|=c:o=a}t.push(x(o,i))}else t.push(x(r))}return t}function dh(e){return e[ih]}function fh(e,t,n,r){let o=e[Tr];throw t[Ec]&&o.unshift(t[Ec]),e.message=hh(`
`+e.message,o,n,r),e[sh]=o,e[Tr]=null,e}function hh(e,t,n,r=null){e=e&&e.charAt(0)===`
`&&e.charAt(1)==ch?e.slice(2):e;let o=ae(t);if(Array.isArray(t))o=t.map(ae).join(" -> ");else if(typeof t=="object"){let i=[];for(let s in t)if(t.hasOwnProperty(s)){let a=t[s];i.push(s+":"+(typeof a=="string"?JSON.stringify(a):ae(a)))}o=`{${i.join(", ")}}`}return`${n}${r?"("+r+")":""}[${o}]: ${e.replace(ah,`
  `)}`}function Ut(e,t){let n=e.hasOwnProperty(_r);return n?e[_r]:null}function es(e,t){e.forEach(n=>Array.isArray(n)?es(n,t):t(n))}function nu(e,t,n){t>=e.length?e.push(n):e.splice(t,0,n)}function Ar(e,t){return t>=e.length-1?e.pop():e.splice(t,1)[0]}var Ln={},Ht=[],zt=new y(""),ru=new y("",-1),ou=new y(""),Nr=class{get(t,n=Rn){if(n===Rn){let r=new Error(`NullInjectorError: No provider for ${ae(t)}!`);throw r.name="NullInjectorError",r}return n}},iu=function(e){return e[e.OnPush=0]="OnPush",e[e.Default=1]="Default",e}(iu||{}),_e=function(e){return e[e.Emulated=0]="Emulated",e[e.None=2]="None",e[e.ShadowDom=3]="ShadowDom",e}(_e||{}),Je=function(e){return e[e.None=0]="None",e[e.SignalBased=1]="SignalBased",e[e.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",e}(Je||{});function ph(e,t,n){let r=e.length;for(;;){let o=e.indexOf(t,n);if(o===-1)return o;if(o===0||e.charCodeAt(o-1)<=32){let i=t.length;if(o+i===r||e.charCodeAt(o+i)<=32)return o}n=o+1}}function Li(e,t,n){let r=0;for(;r<n.length;){let o=n[r];if(typeof o=="number"){if(o!==0)break;r++;let i=n[r++],s=n[r++],a=n[r++];e.setAttribute(t,s,a,i)}else{let i=o,s=n[++r];Eh(i)?e.setProperty(t,i,s):e.setAttribute(t,i,s),r++}}return r}function su(e){return e===3||e===4||e===6}function Eh(e){return e.charCodeAt(0)===64}function ts(e,t){if(!(t===null||t.length===0))if(e===null||e.length===0)e=t.slice();else{let n=-1;for(let r=0;r<t.length;r++){let o=t[r];typeof o=="number"?n=o:n===0||(n===-1||n===2?mc(e,n,o,null,t[++r]):mc(e,n,o,null,null))}}return e}function mc(e,t,n,r,o){let i=0,s=e.length;if(t===-1)s=-1;else for(;i<e.length;){let a=e[i++];if(typeof a=="number"){if(a===t){s=-1;break}else if(a>t){s=i-1;break}}}for(;i<e.length;){let a=e[i];if(typeof a=="number")break;if(a===n){if(r===null){o!==null&&(e[i+1]=o);return}else if(r===e[i+1]){e[i+2]=o;return}}i++,r!==null&&i++,o!==null&&i++}s!==-1&&(e.splice(s,0,t),i=s+1),e.splice(i++,0,n),r!==null&&e.splice(i++,0,r),o!==null&&e.splice(i++,0,o)}var au="ng-template";function mh(e,t,n,r){let o=0;if(r){for(;o<t.length&&typeof t[o]=="string";o+=2)if(t[o]==="class"&&ph(t[o+1].toLowerCase(),n,0)!==-1)return!0}else if(ns(e))return!1;if(o=t.indexOf(1,o),o>-1){let i;for(;++o<t.length&&typeof(i=t[o])=="string";)if(i.toLowerCase()===n)return!0}return!1}function ns(e){return e.type===4&&e.value!==au}function gh(e,t,n){let r=e.type===4&&!n?au:e.value;return t===r}function Rh(e,t,n){let r=4,o=e.attrs,i=o!==null?vh(o):0,s=!1;for(let a=0;a<t.length;a++){let c=t[a];if(typeof c=="number"){if(!s&&!ye(r)&&!ye(c))return!1;if(s&&ye(c))continue;s=!1,r=c|r&1;continue}if(!s)if(r&4){if(r=2|r&1,c!==""&&!gh(e,c,n)||c===""&&t.length===1){if(ye(r))return!1;s=!0}}else if(r&8){if(o===null||!mh(e,o,c,n)){if(ye(r))return!1;s=!0}}else{let u=t[++a],l=Lh(c,o,ns(e),n);if(l===-1){if(ye(r))return!1;s=!0;continue}if(u!==""){let d;if(l>i?d="":d=o[l+1].toLowerCase(),r&2&&u!==d){if(ye(r))return!1;s=!0}}}}return ye(r)||s}function ye(e){return(e&1)===0}function Lh(e,t,n,r){if(t===null)return-1;let o=0;if(r||!n){let i=!1;for(;o<t.length;){let s=t[o];if(s===e)return o;if(s===3||s===6)i=!0;else if(s===1||s===2){let a=t[++o];for(;typeof a=="string";)a=t[++o];continue}else{if(s===4)break;if(s===0){o+=4;continue}}o+=i?1:2}return-1}else return Sh(t,e)}function bh(e,t,n=!1){for(let r=0;r<t.length;r++)if(Rh(e,t[r],n))return!0;return!1}function vh(e){for(let t=0;t<e.length;t++){let n=e[t];if(su(n))return t}return e.length}function Sh(e,t){let n=e.indexOf(4);if(n>-1)for(n++;n<e.length;){let r=e[n];if(typeof r=="number")return-1;if(r===t)return n;n++}return-1}function gc(e,t){return e?":not("+t.trim()+")":t}function yh(e){let t=e[0],n=1,r=2,o="",i=!1;for(;n<e.length;){let s=e[n];if(typeof s=="string")if(r&2){let a=e[++n];o+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else r&8?o+="."+s:r&4&&(o+=" "+s);else o!==""&&!ye(s)&&(t+=gc(i,o),o=""),r=s,i=i||!ye(r);n++}return o!==""&&(t+=gc(i,o)),t}function Dh(e){return e.map(yh).join(",")}function wh(e){let t=[],n=[],r=1,o=2;for(;r<e.length;){let i=e[r];if(typeof i=="string")o===2?i!==""&&t.push(i,e[++r]):o===8&&n.push(i);else{if(!ye(o))break;o=i}r++}return{attrs:t,classes:n}}function et(e){return Ki(()=>{let t=fu(e),n=$(E({},t),{decls:e.decls,vars:e.vars,template:e.template,consts:e.consts||null,ngContentSelectors:e.ngContentSelectors,onPush:e.changeDetection===iu.OnPush,directiveDefs:null,pipeDefs:null,dependencies:t.standalone&&e.dependencies||null,getStandaloneInjector:null,signals:e.signals??!1,data:e.data||{},encapsulation:e.encapsulation||_e.Emulated,styles:e.styles||Ht,_:null,schemas:e.schemas||null,tView:null,id:""});hu(n);let r=e.dependencies;return n.directiveDefs=Lc(r,!1),n.pipeDefs=Lc(r,!0),n.id=xh(n),n})}function Ch(e){return ut(e)||cu(e)}function Ih(e){return e!==null}function Rc(e,t){if(e==null)return Ln;let n={};for(let r in e)if(e.hasOwnProperty(r)){let o=e[r],i,s,a=Je.None;Array.isArray(o)?(a=o[0],i=o[1],s=o[2]??i):(i=o,s=o),t?(n[i]=a!==Je.None?[r,a]:r,t[i]=s):n[i]=r}return n}function Jr(e){return Ki(()=>{let t=fu(e);return hu(t),t})}function ut(e){return e[Jf]||null}function cu(e){return e[Xf]||null}function uu(e){return e[eh]||null}function lu(e){let t=ut(e)||cu(e)||uu(e);return t!==null?t.standalone:!1}function du(e,t){let n=e[th]||null;if(!n&&t===!0)throw new Error(`Type ${ae(e)} does not have '\u0275mod' property.`);return n}function fu(e){let t={};return{type:e.type,providersResolver:null,factory:null,hostBindings:e.hostBindings||null,hostVars:e.hostVars||0,hostAttrs:e.hostAttrs||null,contentQueries:e.contentQueries||null,declaredInputs:t,inputTransforms:null,inputConfig:e.inputs||Ln,exportAs:e.exportAs||null,standalone:e.standalone===!0,signals:e.signals===!0,selectors:e.selectors||Ht,viewQuery:e.viewQuery||null,features:e.features||null,setInput:null,findHostDirectiveDefs:null,hostDirectives:null,inputs:Rc(e.inputs,t),outputs:Rc(e.outputs),debugInfo:null}}function hu(e){e.features?.forEach(t=>t(e))}function Lc(e,t){if(!e)return null;let n=t?uu:Ch;return()=>(typeof e=="function"?e():e).map(r=>n(r)).filter(Ih)}function xh(e){let t=0,n=[e.selectors,e.ngContentSelectors,e.hostVars,e.hostAttrs,e.consts,e.vars,e.decls,e.encapsulation,e.standalone,e.signals,e.exportAs,JSON.stringify(e.inputs),JSON.stringify(e.outputs),Object.getOwnPropertyNames(e.type.prototype),!!e.contentQueries,!!e.viewQuery].join("|");for(let o of n)t=Math.imul(31,t)+o.charCodeAt(0)<<0;return t+=2147483648,"c"+t}function Xr(e){return{\u0275providers:e}}function Mh(...e){return{\u0275providers:pu(!0,e),\u0275fromNgModule:!0}}function pu(e,...t){let n=[],r=new Set,o,i=s=>{n.push(s)};return es(t,s=>{let a=s;bi(a,i,[],r)&&(o||=[],o.push(a))}),o!==void 0&&Eu(o,i),n}function Eu(e,t){for(let n=0;n<e.length;n++){let{ngModule:r,providers:o}=e[n];rs(o,i=>{t(i,r)})}}function bi(e,t,n,r){if(e=de(e),!e)return!1;let o=null,i=fc(e),s=!i&&ut(e);if(!i&&!s){let c=e.ngModule;if(i=fc(c),i)o=c;else return!1}else{if(s&&!s.standalone)return!1;o=e}let a=r.has(o);if(s){if(a)return!1;if(r.add(o),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let u of c)bi(u,t,n,r)}}else if(i){if(i.imports!=null&&!a){r.add(o);let u;try{es(i.imports,l=>{bi(l,t,n,r)&&(u||=[],u.push(l))})}finally{}u!==void 0&&Eu(u,t)}if(!a){let u=Ut(o)||(()=>new o);t({provide:o,useFactory:u,deps:Ht},o),t({provide:ou,useValue:o,multi:!0},o),t({provide:zt,useValue:()=>x(o),multi:!0},o)}let c=i.providers;if(c!=null&&!a){let u=e;rs(c,l=>{t(l,u)})}}else return!1;return o!==e&&e.providers!==void 0}function rs(e,t){for(let n of e)Xc(n)&&(n=n.\u0275providers),Array.isArray(n)?rs(n,t):t(n)}var _h=j({provide:String,useValue:j});function mu(e){return e!==null&&typeof e=="object"&&_h in e}function Th(e){return!!(e&&e.useExisting)}function Ah(e){return!!(e&&e.useFactory)}function vi(e){return typeof e=="function"}var eo=new y(""),wr={},Nh={},ci;function os(){return ci===void 0&&(ci=new Nr),ci}var pe=class{},bn=class extends pe{get destroyed(){return this._destroyed}constructor(t,n,r,o){super(),this.parent=n,this.source=r,this.scopes=o,this.records=new Map,this._ngOnDestroyHooks=new Set,this._onDestroyHooks=[],this._destroyed=!1,yi(t,s=>this.processProvider(s)),this.records.set(ru,Ft(void 0,this)),o.has("environment")&&this.records.set(pe,Ft(void 0,this));let i=this.records.get(eo);i!=null&&typeof i.value=="string"&&this.scopes.add(i.value),this.injectorDefTypes=new Set(this.get(ou,Ht,D.Self))}destroy(){this.assertNotDestroyed(),this._destroyed=!0;let t=P(null);try{for(let r of this._ngOnDestroyHooks)r.ngOnDestroy();let n=this._onDestroyHooks;this._onDestroyHooks=[];for(let r of n)r()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),P(t)}}onDestroy(t){return this.assertNotDestroyed(),this._onDestroyHooks.push(t),()=>this.removeOnDestroy(t)}runInContext(t){this.assertNotDestroyed();let n=Ye(this),r=le(void 0),o;try{return t()}finally{Ye(n),le(r)}}get(t,n=Rn,r=D.Default){if(this.assertNotDestroyed(),t.hasOwnProperty(pc))return t[pc](this);r=Kr(r);let o,i=Ye(this),s=le(void 0);try{if(!(r&D.SkipSelf)){let c=this.records.get(t);if(c===void 0){let u=Vh(t)&&Qr(t);u&&this.injectableDefInScope(u)?c=Ft(Si(t),wr):c=null,this.records.set(t,c)}if(c!=null)return this.hydrate(t,c)}let a=r&D.Self?os():this.parent;return n=r&D.Optional&&n===Rn?null:n,a.get(t,n)}catch(a){if(a.name==="NullInjectorError"){if((a[Tr]=a[Tr]||[]).unshift(ae(t)),i)throw a;return fh(a,t,"R3InjectorError",this.source)}else throw a}finally{le(s),Ye(i)}}resolveInjectorInitializers(){let t=P(null),n=Ye(this),r=le(void 0),o;try{let i=this.get(zt,Ht,D.Self);for(let s of i)s()}finally{Ye(n),le(r),P(t)}}toString(){let t=[],n=this.records;for(let r of n.keys())t.push(ae(r));return`R3Injector[${t.join(", ")}]`}assertNotDestroyed(){if(this._destroyed)throw new g(205,!1)}processProvider(t){t=de(t);let n=vi(t)?t:de(t&&t.provide),r=Ph(t);if(!vi(t)&&t.multi===!0){let o=this.records.get(n);o||(o=Ft(void 0,wr,!0),o.factory=()=>Ri(o.multi),this.records.set(n,o)),n=t,o.multi.push(t)}this.records.set(n,r)}hydrate(t,n){let r=P(null);try{return n.value===wr&&(n.value=Nh,n.value=n.factory()),typeof n.value=="object"&&n.value&&jh(n.value)&&this._ngOnDestroyHooks.add(n.value),n.value}finally{P(r)}}injectableDefInScope(t){if(!t.providedIn)return!1;let n=de(t.providedIn);return typeof n=="string"?n==="any"||this.scopes.has(n):this.injectorDefTypes.has(n)}removeOnDestroy(t){let n=this._onDestroyHooks.indexOf(t);n!==-1&&this._onDestroyHooks.splice(n,1)}};function Si(e){let t=Qr(e),n=t!==null?t.factory:Ut(e);if(n!==null)return n;if(e instanceof y)throw new g(204,!1);if(e instanceof Function)return Oh(e);throw new g(204,!1)}function Oh(e){if(e.length>0)throw new g(204,!1);let n=Qf(e);return n!==null?()=>n.factory(e):()=>new e}function Ph(e){if(mu(e))return Ft(void 0,e.useValue);{let t=kh(e);return Ft(t,wr)}}function kh(e,t,n){let r;if(vi(e)){let o=de(e);return Ut(o)||Si(o)}else if(mu(e))r=()=>de(e.useValue);else if(Ah(e))r=()=>e.useFactory(...Ri(e.deps||[]));else if(Th(e))r=()=>x(de(e.useExisting));else{let o=de(e&&(e.useClass||e.provide));if(Fh(e))r=()=>new o(...Ri(e.deps));else return Ut(o)||Si(o)}return r}function Ft(e,t,n=!1){return{factory:e,value:t,multi:n?[]:void 0}}function Fh(e){return!!e.deps}function jh(e){return e!==null&&typeof e=="object"&&typeof e.ngOnDestroy=="function"}function Vh(e){return typeof e=="function"||typeof e=="object"&&e instanceof y}function yi(e,t){for(let n of e)Array.isArray(n)?yi(n,t):n&&Xc(n)?yi(n.\u0275providers,t):t(n)}function $e(e,t){e instanceof bn&&e.assertNotDestroyed();let n,r=Ye(e),o=le(void 0);try{return t()}finally{Ye(r),le(o)}}function Bh(){return eu()!==void 0||uh()!=null}function $h(e){return typeof e=="function"}var Ue=0,I=1,R=2,X=3,De=4,we=5,Or=6,Pr=7,je=8,qt=9,Te=10,re=11,vn=12,bc=13,In=14,Ae=15,Sn=16,jt=17,to=18,no=19,gu=20,Qe=21,ui=22,fe=23,lt=25,Ru=1;var dt=7,kr=8,Fr=9,he=10,jr=function(e){return e[e.None=0]="None",e[e.HasTransplantedViews=2]="HasTransplantedViews",e}(jr||{});function Ke(e){return Array.isArray(e)&&typeof e[Ru]=="object"}function He(e){return Array.isArray(e)&&e[Ru]===!0}function Lu(e){return(e.flags&4)!==0}function ro(e){return e.componentOffset>-1}function bu(e){return(e.flags&1)===1}function xn(e){return!!e.template}function Di(e){return(e[R]&512)!==0}var wi=class{constructor(t,n,r){this.previousValue=t,this.currentValue=n,this.firstChange=r}isFirstChange(){return this.firstChange}};function vu(e,t,n,r){t!==null?t.applyValueToInputSignal(t,r):e[n]=r}function Mn(){return Su}function Su(e){return e.type.prototype.ngOnChanges&&(e.setInput=Hh),Uh}Mn.ngInherit=!0;function Uh(){let e=Du(this),t=e?.current;if(t){let n=e.previous;if(n===Ln)e.previous=t;else for(let r in t)n[r]=t[r];e.current=null,this.ngOnChanges(t)}}function Hh(e,t,n,r,o){let i=this.declaredInputs[r],s=Du(e)||zh(e,{previous:Ln,current:null}),a=s.current||(s.current={}),c=s.previous,u=c[i];a[i]=new wi(u&&u.currentValue,n,c===Ln),vu(e,t,o,n)}var yu="__ngSimpleChanges__";function Du(e){return e[yu]||null}function zh(e,t){return e[yu]=t}var vc=null;var xe=function(e,t,n){vc?.(e,t,n)},qh="svg",Gh="math";function Ve(e){for(;Array.isArray(e);)e=e[Ue];return e}function Ee(e,t){return Ve(t[e.index])}function wu(e,t){return e.data[t]}function tt(e,t){let n=t[e];return Ke(n)?n:n[Ue]}function is(e){return(e[R]&128)===128}function Wh(e){return He(e[X])}function Sc(e,t){return t==null?null:e[t]}function Cu(e){e[jt]=0}function Iu(e){e[R]&1024||(e[R]|=1024,is(e)&&io(e))}function oo(e){return!!(e[R]&9216||e[fe]?.dirty)}function Ci(e){e[Te].changeDetectionScheduler?.notify(8),e[R]&64&&(e[R]|=1024),oo(e)&&io(e)}function io(e){e[Te].changeDetectionScheduler?.notify(0);let t=ft(e);for(;t!==null&&!(t[R]&8192||(t[R]|=8192,!is(t)));)t=ft(t)}function xu(e,t){if((e[R]&256)===256)throw new g(911,!1);e[Qe]===null&&(e[Qe]=[]),e[Qe].push(t)}function Zh(e,t){if(e[Qe]===null)return;let n=e[Qe].indexOf(t);n!==-1&&e[Qe].splice(n,1)}function ft(e){let t=e[X];return He(t)?t[X]:t}var A={lFrame:ju(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Mu=!1;function Yh(){return A.lFrame.elementDepthCount}function Qh(){A.lFrame.elementDepthCount++}function Kh(){A.lFrame.elementDepthCount--}function _u(){return A.bindingsEnabled}function Jh(){return A.skipHydrationRootTNode!==null}function Xh(e){return A.skipHydrationRootTNode===e}function ep(){A.skipHydrationRootTNode=null}function W(){return A.lFrame.lView}function Et(){return A.lFrame.tView}function me(){let e=Tu();for(;e!==null&&e.type===64;)e=e.parent;return e}function Tu(){return A.lFrame.currentTNode}function tp(){let e=A.lFrame,t=e.currentTNode;return e.isParent?t:t.parent}function so(e,t){let n=A.lFrame;n.currentTNode=e,n.isParent=t}function Au(){return A.lFrame.isParent}function np(){A.lFrame.isParent=!1}function Nu(){return Mu}function yc(e){Mu=e}function rp(){let e=A.lFrame,t=e.bindingRootIndex;return t===-1&&(t=e.bindingRootIndex=e.tView.bindingStartIndex),t}function op(e){return A.lFrame.bindingIndex=e}function Ou(){return A.lFrame.bindingIndex++}function ip(){return A.lFrame.inI18n}function sp(e,t){let n=A.lFrame;n.bindingIndex=n.bindingRootIndex=e,Ii(t)}function ap(){return A.lFrame.currentDirectiveIndex}function Ii(e){A.lFrame.currentDirectiveIndex=e}function Pu(e){A.lFrame.currentQueryIndex=e}function cp(e){let t=e[I];return t.type===2?t.declTNode:t.type===1?e[we]:null}function ku(e,t,n){if(n&D.SkipSelf){let o=t,i=e;for(;o=o.parent,o===null&&!(n&D.Host);)if(o=cp(i),o===null||(i=i[In],o.type&10))break;if(o===null)return!1;t=o,e=i}let r=A.lFrame=Fu();return r.currentTNode=t,r.lView=e,!0}function ss(e){let t=Fu(),n=e[I];A.lFrame=t,t.currentTNode=n.firstChild,t.lView=e,t.tView=n,t.contextLView=e,t.bindingIndex=n.bindingStartIndex,t.inI18n=!1}function Fu(){let e=A.lFrame,t=e===null?null:e.child;return t===null?ju(e):t}function ju(e){let t={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:e,child:null,inI18n:!1};return e!==null&&(e.child=t),t}function Vu(){let e=A.lFrame;return A.lFrame=e.parent,e.currentTNode=null,e.lView=null,e}var Bu=Vu;function as(){let e=Vu();e.isParent=!0,e.tView=null,e.selectedIndex=-1,e.contextLView=null,e.elementDepthCount=0,e.currentDirectiveIndex=-1,e.currentNamespace=null,e.bindingRootIndex=-1,e.bindingIndex=-1,e.currentQueryIndex=0}function $u(){return A.lFrame.selectedIndex}function ht(e){A.lFrame.selectedIndex=e}function Uu(){let e=A.lFrame;return wu(e.tView,e.selectedIndex)}function up(){return A.lFrame.currentNamespace}var Hu=!0;function zu(){return Hu}function qu(e){Hu=e}function lp(e,t,n){let{ngOnChanges:r,ngOnInit:o,ngDoCheck:i}=t.type.prototype;if(r){let s=Su(t);(n.preOrderHooks??=[]).push(e,s),(n.preOrderCheckHooks??=[]).push(e,s)}o&&(n.preOrderHooks??=[]).push(0-e,o),i&&((n.preOrderHooks??=[]).push(e,i),(n.preOrderCheckHooks??=[]).push(e,i))}function Gu(e,t){for(let n=t.directiveStart,r=t.directiveEnd;n<r;n++){let i=e.data[n].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:u,ngOnDestroy:l}=i;s&&(e.contentHooks??=[]).push(-n,s),a&&((e.contentHooks??=[]).push(n,a),(e.contentCheckHooks??=[]).push(n,a)),c&&(e.viewHooks??=[]).push(-n,c),u&&((e.viewHooks??=[]).push(n,u),(e.viewCheckHooks??=[]).push(n,u)),l!=null&&(e.destroyHooks??=[]).push(n,l)}}function Cr(e,t,n){Wu(e,t,3,n)}function Ir(e,t,n,r){(e[R]&3)===n&&Wu(e,t,n,r)}function li(e,t){let n=e[R];(n&3)===t&&(n&=16383,n+=1,e[R]=n)}function Wu(e,t,n,r){let o=r!==void 0?e[jt]&65535:0,i=r??-1,s=t.length-1,a=0;for(let c=o;c<s;c++)if(typeof t[c+1]=="number"){if(a=t[c],r!=null&&a>=r)break}else t[c]<0&&(e[jt]+=65536),(a<i||i==-1)&&(dp(e,n,t,c),e[jt]=(e[jt]&4294901760)+c+2),c++}function Dc(e,t){xe(4,e,t);let n=P(null);try{t.call(e)}finally{P(n),xe(5,e,t)}}function dp(e,t,n,r){let o=n[r]<0,i=n[r+1],s=o?-n[r]:n[r],a=e[s];o?e[R]>>14<e[jt]>>16&&(e[R]&3)===t&&(e[R]+=16384,Dc(a,i)):Dc(a,i)}var $t=-1,yn=class{constructor(t,n,r){this.factory=t,this.resolving=!1,this.canSeeViewProviders=n,this.injectImpl=r}};function fp(e){return e instanceof yn}function hp(e){return(e.flags&8)!==0}function pp(e){return(e.flags&16)!==0}var di={},xi=class{constructor(t,n){this.injector=t,this.parentInjector=n}get(t,n,r){r=Kr(r);let o=this.injector.get(t,di,r);return o!==di||n===di?o:this.parentInjector.get(t,n,r)}};function Zu(e){return e!==$t}function Vr(e){return e&32767}function Ep(e){return e>>16}function Br(e,t){let n=Ep(e),r=t;for(;n>0;)r=r[In],n--;return r}var Mi=!0;function wc(e){let t=Mi;return Mi=e,t}var mp=256,Yu=mp-1,Qu=5,gp=0,Me={};function Rp(e,t,n){let r;typeof n=="string"?r=n.charCodeAt(0)||0:n.hasOwnProperty(gn)&&(r=n[gn]),r==null&&(r=n[gn]=gp++);let o=r&Yu,i=1<<o;t.data[e+(o>>Qu)]|=i}function Ku(e,t){let n=Ju(e,t);if(n!==-1)return n;let r=t[I];r.firstCreatePass&&(e.injectorIndex=t.length,fi(r.data,e),fi(t,null),fi(r.blueprint,null));let o=cs(e,t),i=e.injectorIndex;if(Zu(o)){let s=Vr(o),a=Br(o,t),c=a[I].data;for(let u=0;u<8;u++)t[i+u]=a[s+u]|c[s+u]}return t[i+8]=o,i}function fi(e,t){e.push(0,0,0,0,0,0,0,0,t)}function Ju(e,t){return e.injectorIndex===-1||e.parent&&e.parent.injectorIndex===e.injectorIndex||t[e.injectorIndex+8]===null?-1:e.injectorIndex}function cs(e,t){if(e.parent&&e.parent.injectorIndex!==-1)return e.parent.injectorIndex;let n=0,r=null,o=t;for(;o!==null;){if(r=rl(o),r===null)return $t;if(n++,o=o[In],r.injectorIndex!==-1)return r.injectorIndex|n<<16}return $t}function Lp(e,t,n){Rp(e,t,n)}function bp(e,t){if(t==="class")return e.classes;if(t==="style")return e.styles;let n=e.attrs;if(n){let r=n.length,o=0;for(;o<r;){let i=n[o];if(su(i))break;if(i===0)o=o+2;else if(typeof i=="number")for(o++;o<r&&typeof n[o]=="string";)o++;else{if(i===t)return n[o+1];o=o+2}}}return null}function Xu(e,t,n){if(n&D.Optional||e!==void 0)return e;Xi(t,"NodeInjector")}function el(e,t,n,r){if(n&D.Optional&&r===void 0&&(r=null),!(n&(D.Self|D.Host))){let o=e[qt],i=le(void 0);try{return o?o.get(t,r,n&D.Optional):tu(t,r,n&D.Optional)}finally{le(i)}}return Xu(r,t,n)}function tl(e,t,n,r=D.Default,o){if(e!==null){if(t[R]&2048&&!(r&D.Self)){let s=wp(e,t,n,r,Me);if(s!==Me)return s}let i=nl(e,t,n,r,Me);if(i!==Me)return i}return el(t,n,r,o)}function nl(e,t,n,r,o){let i=yp(n);if(typeof i=="function"){if(!ku(t,e,r))return r&D.Host?Xu(o,n,r):el(t,n,r,o);try{let s;if(s=i(r),s==null&&!(r&D.Optional))Xi(n);else return s}finally{Bu()}}else if(typeof i=="number"){let s=null,a=Ju(e,t),c=$t,u=r&D.Host?t[Ae][we]:null;for((a===-1||r&D.SkipSelf)&&(c=a===-1?cs(e,t):t[a+8],c===$t||!Ic(r,!1)?a=-1:(s=t[I],a=Vr(c),t=Br(c,t)));a!==-1;){let l=t[I];if(Cc(i,a,l.data)){let d=vp(a,t,n,s,r,u);if(d!==Me)return d}c=t[a+8],c!==$t&&Ic(r,t[I].data[a+8]===u)&&Cc(i,a,t)?(s=l,a=Vr(c),t=Br(c,t)):a=-1}}return o}function vp(e,t,n,r,o,i){let s=t[I],a=s.data[e+8],c=r==null?ro(a)&&Mi:r!=s&&(a.type&3)!==0,u=o&D.Host&&i===a,l=Sp(a,s,n,c,u);return l!==null?Dn(t,s,l,a):Me}function Sp(e,t,n,r,o){let i=e.providerIndexes,s=t.data,a=i&1048575,c=e.directiveStart,u=e.directiveEnd,l=i>>20,d=r?a:a+l,p=o?a+l:u;for(let f=d;f<p;f++){let m=s[f];if(f<c&&n===m||f>=c&&m.type===n)return f}if(o){let f=s[c];if(f&&xn(f)&&f.type===n)return c}return null}function Dn(e,t,n,r){let o=e[n],i=t.data;if(fp(o)){let s=o;s.resolving&&rh(nh(i[n]));let a=wc(s.canSeeViewProviders);s.resolving=!0;let c,u=s.injectImpl?le(s.injectImpl):null,l=ku(e,r,D.Default);try{o=e[n]=s.factory(void 0,i,e,r),t.firstCreatePass&&n>=r.directiveStart&&lp(n,i[n],t)}finally{u!==null&&le(u),wc(a),s.resolving=!1,Bu()}}return o}function yp(e){if(typeof e=="string")return e.charCodeAt(0)||0;let t=e.hasOwnProperty(gn)?e[gn]:void 0;return typeof t=="number"?t>=0?t&Yu:Dp:t}function Cc(e,t,n){let r=1<<e;return!!(n[t+(e>>Qu)]&r)}function Ic(e,t){return!(e&D.Self)&&!(e&D.Host&&t)}var ct=class{constructor(t,n){this._tNode=t,this._lView=n}get(t,n,r){return tl(this._tNode,this._lView,t,Kr(r),n)}};function Dp(){return new ct(me(),W())}function us(e){return Ki(()=>{let t=e.prototype.constructor,n=t[_r]||_i(t),r=Object.prototype,o=Object.getPrototypeOf(e.prototype).constructor;for(;o&&o!==r;){let i=o[_r]||_i(o);if(i&&i!==n)return i;o=Object.getPrototypeOf(o)}return i=>new i})}function _i(e){return Yc(e)?()=>{let t=_i(de(e));return t&&t()}:Ut(e)}function wp(e,t,n,r,o){let i=e,s=t;for(;i!==null&&s!==null&&s[R]&2048&&!(s[R]&512);){let a=nl(i,s,n,r|D.Self,Me);if(a!==Me)return a;let c=i.parent;if(!c){let u=s[gu];if(u){let l=u.get(n,Me,r);if(l!==Me)return l}c=rl(s),s=s[In]}i=c}return o}function rl(e){let t=e[I],n=t.type;return n===2?t.declTNode:n===1?e[we]:null}function ls(e){return bp(me(),e)}function xc(e,t=null,n=null,r){let o=ol(e,t,n,r);return o.resolveInjectorInitializers(),o}function ol(e,t=null,n=null,r,o=new Set){let i=[n||Ht,Mh(e)];return r=r||(typeof e=="object"?void 0:ae(e)),new bn(i,t||os(),r||null,o)}var pt=class e{static{this.THROW_IF_NOT_FOUND=Rn}static{this.NULL=new Nr}static create(t,n){if(Array.isArray(t))return xc({name:""},n,t,"");{let r=t.name??"";return xc({name:r},t.parent,t.providers,r)}}static{this.\u0275prov=b({token:e,providedIn:"any",factory:()=>x(ru)})}static{this.__NG_ELEMENT_ID__=-1}};var Cp=new y("");Cp.__NG_ELEMENT_ID__=e=>{let t=me();if(t===null)throw new g(204,!1);if(t.type&2)return t.value;if(e&D.Optional)return null;throw new g(204,!1)};var Ip="ngOriginalError";function hi(e){return e[Ip]}var il=!0,sl=(()=>{class e{static{this.__NG_ELEMENT_ID__=xp}static{this.__NG_ENV_ID__=n=>n}}return e})(),Ti=class extends sl{constructor(t){super(),this._lView=t}onDestroy(t){return xu(this._lView,t),()=>Zh(this._lView,t)}};function xp(){return new Ti(W())}var Kt=(()=>{class e{constructor(){this.taskId=0,this.pendingTasks=new Set,this.hasPendingTasks=new Z(!1)}get _hasPendingTasks(){return this.hasPendingTasks.value}add(){this._hasPendingTasks||this.hasPendingTasks.next(!0);let n=this.taskId++;return this.pendingTasks.add(n),n}remove(n){this.pendingTasks.delete(n),this.pendingTasks.size===0&&this._hasPendingTasks&&this.hasPendingTasks.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this._hasPendingTasks&&this.hasPendingTasks.next(!1)}static{this.\u0275prov=b({token:e,providedIn:"root",factory:()=>new e})}}return e})();var Ai=class extends K{constructor(t=!1){super(),this.destroyRef=void 0,this.pendingTasks=void 0,this.__isAsync=t,Bh()&&(this.destroyRef=h(sl,{optional:!0})??void 0,this.pendingTasks=h(Kt,{optional:!0})??void 0)}emit(t){let n=P(null);try{super.next(t)}finally{P(n)}}subscribe(t,n,r){let o=t,i=n||(()=>null),s=r;if(t&&typeof t=="object"){let c=t;o=c.next?.bind(c),i=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(i=this.wrapInTimeout(i),o&&(o=this.wrapInTimeout(o)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:o,error:i,complete:s});return t instanceof U&&t.add(a),a}wrapInTimeout(t){return n=>{let r=this.pendingTasks?.add();setTimeout(()=>{t(n),r!==void 0&&this.pendingTasks?.remove(r)})}}},J=Ai;function $r(...e){}function al(e){let t,n;function r(){e=$r;try{n!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(n),t!==void 0&&clearTimeout(t)}catch{}}return t=setTimeout(()=>{e(),r()}),typeof requestAnimationFrame=="function"&&(n=requestAnimationFrame(()=>{e(),r()})),()=>r()}function Mc(e){return queueMicrotask(()=>e()),()=>{e=$r}}var ds="isAngularZone",Ur=ds+"_ID",Mp=0,H=class e{constructor(t){this.hasPendingMacrotasks=!1,this.hasPendingMicrotasks=!1,this.isStable=!0,this.onUnstable=new J(!1),this.onMicrotaskEmpty=new J(!1),this.onStable=new J(!1),this.onError=new J(!1);let{enableLongStackTrace:n=!1,shouldCoalesceEventChangeDetection:r=!1,shouldCoalesceRunChangeDetection:o=!1,scheduleInRootZone:i=il}=t;if(typeof Zone>"u")throw new g(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),n&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!o&&r,s.shouldCoalesceRunChangeDetection=o,s.callbackScheduled=!1,s.scheduleInRootZone=i,Ap(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(ds)===!0}static assertInAngularZone(){if(!e.isInAngularZone())throw new g(909,!1)}static assertNotInAngularZone(){if(e.isInAngularZone())throw new g(909,!1)}run(t,n,r){return this._inner.run(t,n,r)}runTask(t,n,r,o){let i=this._inner,s=i.scheduleEventTask("NgZoneEvent: "+o,t,_p,$r,$r);try{return i.runTask(s,n,r)}finally{i.cancelTask(s)}}runGuarded(t,n,r){return this._inner.runGuarded(t,n,r)}runOutsideAngular(t){return this._outer.run(t)}},_p={};function fs(e){if(e._nesting==0&&!e.hasPendingMicrotasks&&!e.isStable)try{e._nesting++,e.onMicrotaskEmpty.emit(null)}finally{if(e._nesting--,!e.hasPendingMicrotasks)try{e.runOutsideAngular(()=>e.onStable.emit(null))}finally{e.isStable=!0}}}function Tp(e){if(e.isCheckStableRunning||e.callbackScheduled)return;e.callbackScheduled=!0;function t(){al(()=>{e.callbackScheduled=!1,Ni(e),e.isCheckStableRunning=!0,fs(e),e.isCheckStableRunning=!1})}e.scheduleInRootZone?Zone.root.run(()=>{t()}):e._outer.run(()=>{t()}),Ni(e)}function Ap(e){let t=()=>{Tp(e)},n=Mp++;e._inner=e._inner.fork({name:"angular",properties:{[ds]:!0,[Ur]:n,[Ur+n]:!0},onInvokeTask:(r,o,i,s,a,c)=>{if(Np(c))return r.invokeTask(i,s,a,c);try{return _c(e),r.invokeTask(i,s,a,c)}finally{(e.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||e.shouldCoalesceRunChangeDetection)&&t(),Tc(e)}},onInvoke:(r,o,i,s,a,c,u)=>{try{return _c(e),r.invoke(i,s,a,c,u)}finally{e.shouldCoalesceRunChangeDetection&&!e.callbackScheduled&&!Op(c)&&t(),Tc(e)}},onHasTask:(r,o,i,s)=>{r.hasTask(i,s),o===i&&(s.change=="microTask"?(e._hasPendingMicrotasks=s.microTask,Ni(e),fs(e)):s.change=="macroTask"&&(e.hasPendingMacrotasks=s.macroTask))},onHandleError:(r,o,i,s)=>(r.handleError(i,s),e.runOutsideAngular(()=>e.onError.emit(s)),!1)})}function Ni(e){e._hasPendingMicrotasks||(e.shouldCoalesceEventChangeDetection||e.shouldCoalesceRunChangeDetection)&&e.callbackScheduled===!0?e.hasPendingMicrotasks=!0:e.hasPendingMicrotasks=!1}function _c(e){e._nesting++,e.isStable&&(e.isStable=!1,e.onUnstable.emit(null))}function Tc(e){e._nesting--,fs(e)}var Oi=class{constructor(){this.hasPendingMicrotasks=!1,this.hasPendingMacrotasks=!1,this.isStable=!0,this.onUnstable=new J,this.onMicrotaskEmpty=new J,this.onStable=new J,this.onError=new J}run(t,n,r){return t.apply(n,r)}runGuarded(t,n,r){return t.apply(n,r)}runOutsideAngular(t){return t()}runTask(t,n,r,o){return t.apply(n,r)}};function Np(e){return cl(e,"__ignore_ng_zone__")}function Op(e){return cl(e,"__scheduler_tick__")}function cl(e,t){return!Array.isArray(e)||e.length!==1?!1:e[0]?.data?.[t]===!0}var Be=class{constructor(){this._console=console}handleError(t){let n=this._findOriginalError(t);this._console.error("ERROR",t),n&&this._console.error("ORIGINAL ERROR",n)}_findOriginalError(t){let n=t&&hi(t);for(;n&&hi(n);)n=hi(n);return n||null}},Pp=new y("",{providedIn:"root",factory:()=>{let e=h(H),t=h(Be);return n=>e.runOutsideAngular(()=>t.handleError(n))}});function kp(){return hs(me(),W())}function hs(e,t){return new _n(Ee(e,t))}var _n=(()=>{class e{constructor(n){this.nativeElement=n}static{this.__NG_ELEMENT_ID__=kp}}return e})();function ul(e){return(e.flags&128)===128}var ll=new Map,Fp=0;function jp(){return Fp++}function Vp(e){ll.set(e[no],e)}function Pi(e){ll.delete(e[no])}var Ac="__ngContext__";function Gt(e,t){Ke(t)?(e[Ac]=t[no],Vp(t)):e[Ac]=t}function dl(e){return hl(e[vn])}function fl(e){return hl(e[De])}function hl(e){for(;e!==null&&!He(e);)e=e[De];return e}var ki;function pl(e){ki=e}function Bp(){if(ki!==void 0)return ki;if(typeof document<"u")return document;throw new g(210,!1)}var ps=new y("",{providedIn:"root",factory:()=>$p}),$p="ng",Es=new y(""),Jt=new y("",{providedIn:"platform",factory:()=>"unknown"});var ms=new y("",{providedIn:"root",factory:()=>Bp().body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Up="h",Hp="b";var zp=()=>null;function gs(e,t,n=!1){return zp(e,t,n)}var El=!1,qp=new y("",{providedIn:"root",factory:()=>El});var yr;function Gp(){if(yr===void 0&&(yr=null,mi.trustedTypes))try{yr=mi.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:e=>e,createScript:e=>e,createScriptURL:e=>e})}catch{}return yr}function Nc(e){return Gp()?.createScriptURL(e)||e}var Hr=class{constructor(t){this.changingThisBreaksApplicationSecurity=t}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Wc})`}};function ao(e){return e instanceof Hr?e.changingThisBreaksApplicationSecurity:e}function Rs(e,t){let n=Wp(e);if(n!=null&&n!==t){if(n==="ResourceURL"&&t==="URL")return!0;throw new Error(`Required a safe ${t}, got a ${n} (see ${Wc})`)}return n===t}function Wp(e){return e instanceof Hr&&e.getTypeName()||null}var Zp=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function ml(e){return e=String(e),e.match(Zp)?e:"unsafe:"+e}var co=function(e){return e[e.NONE=0]="NONE",e[e.HTML=1]="HTML",e[e.STYLE=2]="STYLE",e[e.SCRIPT=3]="SCRIPT",e[e.URL=4]="URL",e[e.RESOURCE_URL=5]="RESOURCE_URL",e}(co||{});function Yp(e){let t=Rl();return t?t.sanitize(co.URL,e)||"":Rs(e,"URL")?ao(e):ml(Ji(e))}function Qp(e){let t=Rl();if(t)return Nc(t.sanitize(co.RESOURCE_URL,e)||"");if(Rs(e,"ResourceURL"))return Nc(ao(e));throw new g(904,!1)}function Kp(e,t){return t==="src"&&(e==="embed"||e==="frame"||e==="iframe"||e==="media"||e==="script")||t==="href"&&(e==="base"||e==="link")?Qp:Yp}function gl(e,t,n){return Kp(t,n)(e)}function Rl(){let e=W();return e&&e[Te].sanitizer}function Ll(e){return e instanceof Function?e():e}var mt=function(e){return e[e.Important=1]="Important",e[e.DashCase=2]="DashCase",e}(mt||{}),Jp;function Ls(e,t){return Jp(e,t)}function Vt(e,t,n,r,o){if(r!=null){let i,s=!1;He(r)?i=r:Ke(r)&&(s=!0,r=r[Ue]);let a=Ve(r);e===0&&n!==null?o==null?Dl(t,n,a):zr(t,n,a,o||null,!0):e===1&&n!==null?zr(t,n,a,o||null,!0):e===2?fE(t,a,s):e===3&&t.destroyNode(a),i!=null&&pE(t,e,i,n,o)}}function Xp(e,t){return e.createText(t)}function bl(e,t,n){return e.createElement(t,n)}function eE(e,t){vl(e,t),t[Ue]=null,t[we]=null}function tE(e,t,n,r,o,i){r[Ue]=o,r[we]=t,uo(e,r,n,1,o,i)}function vl(e,t){t[Te].changeDetectionScheduler?.notify(9),uo(e,t,t[re],2,null,null)}function nE(e){let t=e[vn];if(!t)return pi(e[I],e);for(;t;){let n=null;if(Ke(t))n=t[vn];else{let r=t[he];r&&(n=r)}if(!n){for(;t&&!t[De]&&t!==e;)Ke(t)&&pi(t[I],t),t=t[X];t===null&&(t=e),Ke(t)&&pi(t[I],t),n=t&&t[De]}t=n}}function rE(e,t,n,r){let o=he+r,i=n.length;r>0&&(n[o-1][De]=t),r<i-he?(t[De]=n[o],nu(n,he+r,t)):(n.push(t),t[De]=null),t[X]=n;let s=t[Sn];s!==null&&n!==s&&Sl(s,t);let a=t[to];a!==null&&a.insertView(e),Ci(t),t[R]|=128}function Sl(e,t){let n=e[Fr],r=t[X];if(Ke(r))e[R]|=jr.HasTransplantedViews;else{let o=r[X][Ae];t[Ae]!==o&&(e[R]|=jr.HasTransplantedViews)}n===null?e[Fr]=[t]:n.push(t)}function bs(e,t){let n=e[Fr],r=n.indexOf(t);n.splice(r,1)}function Fi(e,t){if(e.length<=he)return;let n=he+t,r=e[n];if(r){let o=r[Sn];o!==null&&o!==e&&bs(o,r),t>0&&(e[n-1][De]=r[De]);let i=Ar(e,he+t);eE(r[I],r);let s=i[to];s!==null&&s.detachView(i[I]),r[X]=null,r[De]=null,r[R]&=-129}return r}function yl(e,t){if(!(t[R]&256)){let n=t[re];n.destroyNode&&uo(e,t,n,3,null,null),nE(t)}}function pi(e,t){if(t[R]&256)return;let n=P(null);try{t[R]&=-129,t[R]|=256,t[fe]&&Uo(t[fe]),iE(e,t),oE(e,t),t[I].type===1&&t[re].destroy();let r=t[Sn];if(r!==null&&He(t[X])){r!==t[X]&&bs(r,t);let o=t[to];o!==null&&o.detachView(e)}Pi(t)}finally{P(n)}}function oE(e,t){let n=e.cleanup,r=t[Pr];if(n!==null)for(let i=0;i<n.length-1;i+=2)if(typeof n[i]=="string"){let s=n[i+3];s>=0?r[s]():r[-s].unsubscribe(),i+=2}else{let s=r[n[i+1]];n[i].call(s)}r!==null&&(t[Pr]=null);let o=t[Qe];if(o!==null){t[Qe]=null;for(let i=0;i<o.length;i++){let s=o[i];s()}}}function iE(e,t){let n;if(e!=null&&(n=e.destroyHooks)!=null)for(let r=0;r<n.length;r+=2){let o=t[n[r]];if(!(o instanceof yn)){let i=n[r+1];if(Array.isArray(i))for(let s=0;s<i.length;s+=2){let a=o[i[s]],c=i[s+1];xe(4,a,c);try{c.call(a)}finally{xe(5,a,c)}}else{xe(4,o,i);try{i.call(o)}finally{xe(5,o,i)}}}}}function sE(e,t,n){return aE(e,t.parent,n)}function aE(e,t,n){let r=t;for(;r!==null&&r.type&168;)t=r,r=t.parent;if(r===null)return n[Ue];{let{componentOffset:o}=r;if(o>-1){let{encapsulation:i}=e.data[r.directiveStart+o];if(i===_e.None||i===_e.Emulated)return null}return Ee(r,n)}}function zr(e,t,n,r,o){e.insertBefore(t,n,r,o)}function Dl(e,t,n){e.appendChild(t,n)}function Oc(e,t,n,r,o){r!==null?zr(e,t,n,r,o):Dl(e,t,n)}function wl(e,t){return e.parentNode(t)}function cE(e,t){return e.nextSibling(t)}function uE(e,t,n){return dE(e,t,n)}function lE(e,t,n){return e.type&40?Ee(e,n):null}var dE=lE,Pc;function Cl(e,t,n,r){let o=sE(e,r,t),i=t[re],s=r.parent||t[we],a=uE(s,r,t);if(o!=null)if(Array.isArray(n))for(let c=0;c<n.length;c++)Oc(i,o,n[c],a,!1);else Oc(i,o,n,a,!1);Pc!==void 0&&Pc(i,r,t,n,o)}function En(e,t){if(t!==null){let n=t.type;if(n&3)return Ee(t,e);if(n&4)return ji(-1,e[t.index]);if(n&8){let r=t.child;if(r!==null)return En(e,r);{let o=e[t.index];return He(o)?ji(-1,o):Ve(o)}}else{if(n&128)return En(e,t.next);if(n&32)return Ls(t,e)()||Ve(e[t.index]);{let r=Il(e,t);if(r!==null){if(Array.isArray(r))return r[0];let o=ft(e[Ae]);return En(o,r)}else return En(e,t.next)}}}return null}function Il(e,t){if(t!==null){let r=e[Ae][we],o=t.projection;return r.projection[o]}return null}function ji(e,t){let n=he+e+1;if(n<t.length){let r=t[n],o=r[I].firstChild;if(o!==null)return En(r,o)}return t[dt]}function fE(e,t,n){e.removeChild(null,t,n)}function vs(e,t,n,r,o,i,s){for(;n!=null;){if(n.type===128){n=n.next;continue}let a=r[n.index],c=n.type;if(s&&t===0&&(a&&Gt(Ve(a),r),n.flags|=2),(n.flags&32)!==32)if(c&8)vs(e,t,n.child,r,o,i,!1),Vt(t,e,o,a,i);else if(c&32){let u=Ls(n,r),l;for(;l=u();)Vt(t,e,o,l,i);Vt(t,e,o,a,i)}else c&16?hE(e,t,r,n,o,i):Vt(t,e,o,a,i);n=s?n.projectionNext:n.next}}function uo(e,t,n,r,o,i){vs(n,r,e.firstChild,t,o,i,!1)}function hE(e,t,n,r,o,i){let s=n[Ae],c=s[we].projection[r.projection];if(Array.isArray(c))for(let u=0;u<c.length;u++){let l=c[u];Vt(t,e,o,l,i)}else{let u=c,l=s[X];ul(r)&&(u.flags|=128),vs(e,t,u,l,o,i,!0)}}function pE(e,t,n,r,o){let i=n[dt],s=Ve(n);i!==s&&Vt(t,e,r,i,o);for(let a=he;a<n.length;a++){let c=n[a];uo(c[I],c,e,t,r,i)}}function EE(e,t,n){e.setAttribute(t,"style",n)}function xl(e,t,n){n===""?e.removeAttribute(t,"class"):e.setAttribute(t,"class",n)}function Ml(e,t,n){let{mergedAttrs:r,classes:o,styles:i}=n;r!==null&&Li(e,t,r),o!==null&&xl(e,t,o),i!==null&&EE(e,t,i)}var Ss={};function _l(e=1){Tl(Et(),W(),$u()+e,!1)}function Tl(e,t,n,r){if(!r)if((t[R]&3)===3){let i=e.preOrderCheckHooks;i!==null&&Cr(t,i,n)}else{let i=e.preOrderHooks;i!==null&&Ir(t,i,0,n)}ht(n)}function Ne(e,t=D.Default){let n=W();if(n===null)return x(e,t);let r=me();return tl(r,n,de(e),t)}function Al(e,t,n,r,o,i){let s=P(null);try{let a=null;o&Je.SignalBased&&(a=t[r][Aa]),a!==null&&a.transformFn!==void 0&&(i=a.transformFn(i)),o&Je.HasDecoratorInputTransform&&(i=e.inputTransforms[r].call(t,i)),e.setInput!==null?e.setInput(t,a,i,n,r):vu(t,a,r,i)}finally{P(s)}}function mE(e,t){let n=e.hostBindingOpCodes;if(n!==null)try{for(let r=0;r<n.length;r++){let o=n[r];if(o<0)ht(~o);else{let i=o,s=n[++r],a=n[++r];sp(s,i);let c=t[i];a(2,c)}}}finally{ht(-1)}}function ys(e,t,n,r,o,i,s,a,c,u,l){let d=t.blueprint.slice();return d[Ue]=o,d[R]=r|4|128|8|64,(u!==null||e&&e[R]&2048)&&(d[R]|=2048),Cu(d),d[X]=d[In]=e,d[je]=n,d[Te]=s||e&&e[Te],d[re]=a||e&&e[re],d[qt]=c||e&&e[qt]||null,d[we]=i,d[no]=jp(),d[Or]=l,d[gu]=u,d[Ae]=t.type==2?e[Ae]:d,d}function Ds(e,t,n,r,o){let i=e.data[t];if(i===null)i=gE(e,t,n,r,o),ip()&&(i.flags|=32);else if(i.type&64){i.type=n,i.value=r,i.attrs=o;let s=tp();i.injectorIndex=s===null?-1:s.injectorIndex}return so(i,!0),i}function gE(e,t,n,r,o){let i=Tu(),s=Au(),a=s?i:i&&i.parent,c=e.data[t]=DE(e,a,n,t,r,o);return e.firstChild===null&&(e.firstChild=c),i!==null&&(s?i.child==null&&c.parent!==null&&(i.child=c):i.next===null&&(i.next=c,c.prev=i)),c}function Nl(e,t,n,r){if(n===0)return-1;let o=t.length;for(let i=0;i<n;i++)t.push(r),e.blueprint.push(r),e.data.push(null);return o}function Ol(e,t,n,r,o){let i=$u(),s=r&2;try{ht(-1),s&&t.length>lt&&Tl(e,t,lt,!1),xe(s?2:0,o),n(r,o)}finally{ht(i),xe(s?3:1,o)}}function Pl(e,t,n){if(Lu(t)){let r=P(null);try{let o=t.directiveStart,i=t.directiveEnd;for(let s=o;s<i;s++){let a=e.data[s];if(a.contentQueries){let c=n[s];a.contentQueries(1,c,s)}}}finally{P(r)}}}function RE(e,t,n){_u()&&(AE(e,t,n,Ee(n,t)),(n.flags&64)===64&&Vl(e,t,n))}function LE(e,t,n=Ee){let r=t.localNames;if(r!==null){let o=t.index+1;for(let i=0;i<r.length;i+=2){let s=r[i+1],a=s===-1?n(t,e):e[s];e[o++]=a}}}function kl(e){let t=e.tView;return t===null||t.incompleteFirstPass?e.tView=Fl(1,null,e.template,e.decls,e.vars,e.directiveDefs,e.pipeDefs,e.viewQuery,e.schemas,e.consts,e.id):t}function Fl(e,t,n,r,o,i,s,a,c,u,l){let d=lt+r,p=d+o,f=bE(d,p),m=typeof u=="function"?u():u;return f[I]={type:e,blueprint:f,template:n,queries:null,viewQuery:a,declTNode:t,data:f.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:p,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof i=="function"?i():i,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:m,incompleteFirstPass:!1,ssrId:l}}function bE(e,t){let n=[];for(let r=0;r<t;r++)n.push(r<e?null:Ss);return n}function vE(e,t,n,r){let i=r.get(qp,El)||n===_e.ShadowDom,s=e.selectRootElement(t,i);return SE(s),s}function SE(e){yE(e)}var yE=()=>null;function DE(e,t,n,r,o,i){let s=t?t.injectorIndex:-1,a=0;return Jh()&&(a|=128),{type:n,index:r,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,propertyBindings:null,flags:a,providerIndexes:0,value:o,attrs:i,mergedAttrs:null,localNames:null,initialInputs:void 0,inputs:null,outputs:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:t,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function kc(e,t,n,r,o){for(let i in t){if(!t.hasOwnProperty(i))continue;let s=t[i];if(s===void 0)continue;r??={};let a,c=Je.None;Array.isArray(s)?(a=s[0],c=s[1]):a=s;let u=i;if(o!==null){if(!o.hasOwnProperty(i))continue;u=o[i]}e===0?Fc(r,n,u,a,c):Fc(r,n,u,a)}return r}function Fc(e,t,n,r,o){let i;e.hasOwnProperty(n)?(i=e[n]).push(t,r):i=e[n]=[t,r],o!==void 0&&i.push(o)}function wE(e,t,n){let r=t.directiveStart,o=t.directiveEnd,i=e.data,s=t.attrs,a=[],c=null,u=null;for(let l=r;l<o;l++){let d=i[l],p=n?n.get(d):null,f=p?p.inputs:null,m=p?p.outputs:null;c=kc(0,d.inputs,l,c,f),u=kc(1,d.outputs,l,u,m);let w=c!==null&&s!==null&&!ns(t)?HE(c,l,s):null;a.push(w)}c!==null&&(c.hasOwnProperty("class")&&(t.flags|=8),c.hasOwnProperty("style")&&(t.flags|=16)),t.initialInputs=a,t.inputs=c,t.outputs=u}function CE(e){return e==="class"?"className":e==="for"?"htmlFor":e==="formaction"?"formAction":e==="innerHtml"?"innerHTML":e==="readonly"?"readOnly":e==="tabindex"?"tabIndex":e}function IE(e,t,n,r,o,i,s,a){let c=Ee(t,n),u=t.inputs,l;!a&&u!=null&&(l=u[r])?(Cs(e,n,l,r,o),ro(t)&&xE(n,t.index)):t.type&3?(r=CE(r),o=s!=null?s(o,t.value||"",r):o,i.setProperty(c,r,o)):t.type&12}function xE(e,t){let n=tt(t,e);n[R]&16||(n[R]|=64)}function ME(e,t,n,r){if(_u()){let o=r===null?null:{"":-1},i=OE(e,n),s,a;i===null?s=a=null:[s,a]=i,s!==null&&jl(e,t,n,s,o,a),o&&PE(n,r,o)}n.mergedAttrs=ts(n.mergedAttrs,n.attrs)}function jl(e,t,n,r,o,i){for(let u=0;u<r.length;u++)Lp(Ku(n,t),e,r[u].type);FE(n,e.data.length,r.length);for(let u=0;u<r.length;u++){let l=r[u];l.providersResolver&&l.providersResolver(l)}let s=!1,a=!1,c=Nl(e,t,r.length,null);for(let u=0;u<r.length;u++){let l=r[u];n.mergedAttrs=ts(n.mergedAttrs,l.hostAttrs),jE(e,n,t,c,l),kE(c,l,o),l.contentQueries!==null&&(n.flags|=4),(l.hostBindings!==null||l.hostAttrs!==null||l.hostVars!==0)&&(n.flags|=64);let d=l.type.prototype;!s&&(d.ngOnChanges||d.ngOnInit||d.ngDoCheck)&&((e.preOrderHooks??=[]).push(n.index),s=!0),!a&&(d.ngOnChanges||d.ngDoCheck)&&((e.preOrderCheckHooks??=[]).push(n.index),a=!0),c++}wE(e,n,i)}function _E(e,t,n,r,o){let i=o.hostBindings;if(i){let s=e.hostBindingOpCodes;s===null&&(s=e.hostBindingOpCodes=[]);let a=~t.index;TE(s)!=a&&s.push(a),s.push(n,r,i)}}function TE(e){let t=e.length;for(;t>0;){let n=e[--t];if(typeof n=="number"&&n<0)return n}return 0}function AE(e,t,n,r){let o=n.directiveStart,i=n.directiveEnd;ro(n)&&VE(t,n,e.data[o+n.componentOffset]),e.firstCreatePass||Ku(n,t),Gt(r,t);let s=n.initialInputs;for(let a=o;a<i;a++){let c=e.data[a],u=Dn(t,e,a,n);if(Gt(u,t),s!==null&&UE(t,a-o,u,c,n,s),xn(c)){let l=tt(n.index,t);l[je]=Dn(t,e,a,n)}}}function Vl(e,t,n){let r=n.directiveStart,o=n.directiveEnd,i=n.index,s=ap();try{ht(i);for(let a=r;a<o;a++){let c=e.data[a],u=t[a];Ii(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&NE(c,u)}}finally{ht(-1),Ii(s)}}function NE(e,t){e.hostBindings!==null&&e.hostBindings(1,t)}function OE(e,t){let n=e.directiveRegistry,r=null,o=null;if(n)for(let i=0;i<n.length;i++){let s=n[i];if(bh(t,s.selectors,!1))if(r||(r=[]),xn(s))if(s.findHostDirectiveDefs!==null){let a=[];o=o||new Map,s.findHostDirectiveDefs(s,a,o),r.unshift(...a,s);let c=a.length;Vi(e,t,c)}else r.unshift(s),Vi(e,t,0);else o=o||new Map,s.findHostDirectiveDefs?.(s,r,o),r.push(s)}return r===null?null:[r,o]}function Vi(e,t,n){t.componentOffset=n,(e.components??=[]).push(t.index)}function PE(e,t,n){if(t){let r=e.localNames=[];for(let o=0;o<t.length;o+=2){let i=n[t[o+1]];if(i==null)throw new g(-301,!1);r.push(t[o],i)}}}function kE(e,t,n){if(n){if(t.exportAs)for(let r=0;r<t.exportAs.length;r++)n[t.exportAs[r]]=e;xn(t)&&(n[""]=e)}}function FE(e,t,n){e.flags|=1,e.directiveStart=t,e.directiveEnd=t+n,e.providerIndexes=t}function jE(e,t,n,r,o){e.data[r]=o;let i=o.factory||(o.factory=Ut(o.type,!0)),s=new yn(i,xn(o),Ne);e.blueprint[r]=s,n[r]=s,_E(e,t,r,Nl(e,n,o.hostVars,Ss),o)}function VE(e,t,n){let r=Ee(t,e),o=kl(n),i=e[Te].rendererFactory,s=16;n.signals?s=4096:n.onPush&&(s=64);let a=ws(e,ys(e,o,null,s,r,t,null,i.createRenderer(r,n),null,null,null));e[t.index]=a}function BE(e,t,n,r,o,i){let s=Ee(e,t);$E(t[re],s,i,e.value,n,r,o)}function $E(e,t,n,r,o,i,s){if(i==null)e.removeAttribute(t,o,n);else{let a=s==null?Ji(i):s(i,r||"",o);e.setAttribute(t,o,a,n)}}function UE(e,t,n,r,o,i){let s=i[t];if(s!==null)for(let a=0;a<s.length;){let c=s[a++],u=s[a++],l=s[a++],d=s[a++];Al(r,n,c,u,l,d)}}function HE(e,t,n){let r=null,o=0;for(;o<n.length;){let i=n[o];if(i===0){o+=4;continue}else if(i===5){o+=2;continue}if(typeof i=="number")break;if(e.hasOwnProperty(i)){r===null&&(r=[]);let s=e[i];for(let a=0;a<s.length;a+=3)if(s[a]===t){r.push(i,s[a+1],s[a+2],n[o+1]);break}}o+=2}return r}function zE(e,t,n,r){return[e,!0,0,t,null,r,null,n,null,null]}function Bl(e,t){let n=e.contentQueries;if(n!==null){let r=P(null);try{for(let o=0;o<n.length;o+=2){let i=n[o],s=n[o+1];if(s!==-1){let a=e.data[s];Pu(i),a.contentQueries(2,t[s],s)}}}finally{P(r)}}}function ws(e,t){return e[vn]?e[bc][De]=t:e[vn]=t,e[bc]=t,t}function Bi(e,t,n){Pu(0);let r=P(null);try{t(e,n)}finally{P(r)}}function qE(e){return e[Pr]??=[]}function GE(e){return e.cleanup??=[]}function $l(e,t){let n=e[qt],r=n?n.get(Be,null):null;r&&r.handleError(t)}function Cs(e,t,n,r,o){for(let i=0;i<n.length;){let s=n[i++],a=n[i++],c=n[i++],u=t[s],l=e.data[s];Al(l,u,r,a,c,o)}}function WE(e,t){let n=tt(t,e),r=n[I];ZE(r,n);let o=n[Ue];o!==null&&n[Or]===null&&(n[Or]=gs(o,n[qt])),Ul(r,n,n[je])}function ZE(e,t){for(let n=t.length;n<e.blueprint.length;n++)t.push(e.blueprint[n])}function Ul(e,t,n){ss(t);try{let r=e.viewQuery;r!==null&&Bi(1,r,n);let o=e.template;o!==null&&Ol(e,t,o,1,n),e.firstCreatePass&&(e.firstCreatePass=!1),t[to]?.finishViewCreation(e),e.staticContentQueries&&Bl(e,t),e.staticViewQueries&&Bi(2,e.viewQuery,n);let i=e.components;i!==null&&YE(t,i)}catch(r){throw e.firstCreatePass&&(e.incompleteFirstPass=!0,e.firstCreatePass=!1),r}finally{t[R]&=-5,as()}}function YE(e,t){for(let n=0;n<t.length;n++)WE(e,t[n])}function jc(e,t){return!t||t.firstChild===null||ul(e)}function QE(e,t,n,r=!0){let o=t[I];if(rE(o,t,e,n),r){let s=ji(n,e),a=t[re],c=wl(a,e[dt]);c!==null&&tE(o,e[we],a,t,c,s)}let i=t[Or];i!==null&&i.firstChild!==null&&(i.firstChild=null)}function qr(e,t,n,r,o=!1){for(;n!==null;){if(n.type===128){n=o?n.projectionNext:n.next;continue}let i=t[n.index];i!==null&&r.push(Ve(i)),He(i)&&KE(i,r);let s=n.type;if(s&8)qr(e,t,n.child,r);else if(s&32){let a=Ls(n,t),c;for(;c=a();)r.push(c)}else if(s&16){let a=Il(t,n);if(Array.isArray(a))r.push(...a);else{let c=ft(t[Ae]);qr(c[I],c,a,r,!0)}}n=o?n.projectionNext:n.next}return r}function KE(e,t){for(let n=he;n<e.length;n++){let r=e[n],o=r[I].firstChild;o!==null&&qr(r[I],r,o,t)}e[dt]!==e[Ue]&&t.push(e[dt])}var Hl=[];function JE(e){return e[fe]??XE(e)}function XE(e){let t=Hl.pop()??Object.create(tm);return t.lView=e,t}function em(e){e.lView[fe]!==e&&(e.lView=null,Hl.push(e))}var tm=$(E({},Vo),{consumerIsAlwaysLive:!0,consumerMarkedDirty:e=>{io(e.lView)},consumerOnSignalRead(){this.lView[fe]=this}});function nm(e){let t=e[fe]??Object.create(rm);return t.lView=e,t}var rm=$(E({},Vo),{consumerIsAlwaysLive:!0,consumerMarkedDirty:e=>{let t=ft(e.lView);for(;t&&!zl(t[I]);)t=ft(t);t&&Iu(t)},consumerOnSignalRead(){this.lView[fe]=this}});function zl(e){return e.type!==2}var om=100;function ql(e,t=!0,n=0){let r=e[Te],o=r.rendererFactory,i=!1;i||o.begin?.();try{im(e,n)}catch(s){throw t&&$l(e,s),s}finally{i||(o.end?.(),r.inlineEffectRunner?.flush())}}function im(e,t){let n=Nu();try{yc(!0),$i(e,t);let r=0;for(;oo(e);){if(r===om)throw new g(103,!1);r++,$i(e,1)}}finally{yc(n)}}function sm(e,t,n,r){let o=t[R];if((o&256)===256)return;let i=!1,s=!1;!i&&t[Te].inlineEffectRunner?.flush(),ss(t);let a=!0,c=null,u=null;i||(zl(e)?(u=JE(t),c=Bo(u)):Na()===null?(a=!1,u=nm(t),c=Bo(u)):t[fe]&&(Uo(t[fe]),t[fe]=null));try{Cu(t),op(e.bindingStartIndex),n!==null&&Ol(e,t,n,2,r);let l=(o&3)===3;if(!i)if(l){let f=e.preOrderCheckHooks;f!==null&&Cr(t,f,null)}else{let f=e.preOrderHooks;f!==null&&Ir(t,f,0,null),li(t,0)}if(s||am(t),Gl(t,0),e.contentQueries!==null&&Bl(e,t),!i)if(l){let f=e.contentCheckHooks;f!==null&&Cr(t,f)}else{let f=e.contentHooks;f!==null&&Ir(t,f,1),li(t,1)}mE(e,t);let d=e.components;d!==null&&Zl(t,d,0);let p=e.viewQuery;if(p!==null&&Bi(2,p,r),!i)if(l){let f=e.viewCheckHooks;f!==null&&Cr(t,f)}else{let f=e.viewHooks;f!==null&&Ir(t,f,2),li(t,2)}if(e.firstUpdatePass===!0&&(e.firstUpdatePass=!1),t[ui]){for(let f of t[ui])f();t[ui]=null}i||(t[R]&=-73)}catch(l){throw i||io(t),l}finally{u!==null&&(Oa(u,c),a&&em(u)),as()}}function Gl(e,t){for(let n=dl(e);n!==null;n=fl(n))for(let r=he;r<n.length;r++){let o=n[r];Wl(o,t)}}function am(e){for(let t=dl(e);t!==null;t=fl(t)){if(!(t[R]&jr.HasTransplantedViews))continue;let n=t[Fr];for(let r=0;r<n.length;r++){let o=n[r];Iu(o)}}}function cm(e,t,n){let r=tt(t,e);Wl(r,n)}function Wl(e,t){is(e)&&$i(e,t)}function $i(e,t){let r=e[I],o=e[R],i=e[fe],s=!!(t===0&&o&16);if(s||=!!(o&64&&t===0),s||=!!(o&1024),s||=!!(i?.dirty&&$o(i)),s||=!1,i&&(i.dirty=!1),e[R]&=-9217,s)sm(r,e,r.template,e[je]);else if(o&8192){Gl(e,1);let a=r.components;a!==null&&Zl(e,a,1)}}function Zl(e,t,n){for(let r=0;r<t.length;r++)cm(e,t[r],n)}function Is(e,t){let n=Nu()?64:1088;for(e[Te].changeDetectionScheduler?.notify(t);e;){e[R]|=n;let r=ft(e);if(Di(e)&&!r)return e;e=r}return null}var Wt=class{get rootNodes(){let t=this._lView,n=t[I];return qr(n,t,n.firstChild,[])}constructor(t,n,r=!0){this._lView=t,this._cdRefInjectingView=n,this.notifyErrorHandler=r,this._appRef=null,this._attachedToViewContainer=!1}get context(){return this._lView[je]}set context(t){this._lView[je]=t}get destroyed(){return(this._lView[R]&256)===256}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let t=this._lView[X];if(He(t)){let n=t[kr],r=n?n.indexOf(this):-1;r>-1&&(Fi(t,r),Ar(n,r))}this._attachedToViewContainer=!1}yl(this._lView[I],this._lView)}onDestroy(t){xu(this._lView,t)}markForCheck(){Is(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[R]&=-129}reattach(){Ci(this._lView),this._lView[R]|=128}detectChanges(){this._lView[R]|=1024,ql(this._lView,this.notifyErrorHandler)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new g(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let t=Di(this._lView),n=this._lView[Sn];n!==null&&!t&&bs(n,this._lView),vl(this._lView[I],this._lView)}attachToAppRef(t){if(this._attachedToViewContainer)throw new g(902,!1);this._appRef=t;let n=Di(this._lView),r=this._lView[Sn];r!==null&&!n&&Sl(r,this._lView),Ci(this._lView)}};var cy=new RegExp(`^(\\d+)*(${Hp}|${Up})*(.*)`);var um=()=>null;function Vc(e,t){return um(e,t)}var Zt=class{},lo=new y("",{providedIn:"root",factory:()=>!1});var Yl=new y(""),Ql=new y(""),Ui=class{},Gr=class{};function lm(e){let t=Error(`No component factory found for ${ae(e)}.`);return t[dm]=e,t}var dm="ngComponent";var Hi=class{resolveComponentFactory(t){throw lm(t)}},Yt=class{static{this.NULL=new Hi}},Qt=class{},fo=(()=>{class e{constructor(){this.destroyNode=null}static{this.__NG_ELEMENT_ID__=()=>fm()}}return e})();function fm(){let e=W(),t=me(),n=tt(t.index,e);return(Ke(n)?n:e)[re]}var hm=(()=>{class e{static{this.\u0275prov=b({token:e,providedIn:"root",factory:()=>null})}}return e})();function zi(e,t,n){let r=n?e.styles:null,o=n?e.classes:null,i=0;if(t!==null)for(let s=0;s<t.length;s++){let a=t[s];if(typeof a=="number")i=a;else if(i==1)o=lc(o,a);else if(i==2){let c=a,u=t[++s];r=lc(r,c+": "+u+";")}}n?e.styles=r:e.stylesWithoutHost=r,n?e.classes=o:e.classesWithoutHost=o}var Wr=class extends Yt{constructor(t){super(),this.ngModule=t}resolveComponentFactory(t){let n=ut(t);return new wn(n,this.ngModule)}};function Bc(e,t){let n=[];for(let r in e){if(!e.hasOwnProperty(r))continue;let o=e[r];if(o===void 0)continue;let i=Array.isArray(o),s=i?o[0]:o,a=i?o[1]:Je.None;t?n.push({propName:s,templateName:r,isSignal:(a&Je.SignalBased)!==0}):n.push({propName:s,templateName:r})}return n}function pm(e){let t=e.toLowerCase();return t==="svg"?qh:t==="math"?Gh:null}var wn=class extends Gr{get inputs(){let t=this.componentDef,n=t.inputTransforms,r=Bc(t.inputs,!0);if(n!==null)for(let o of r)n.hasOwnProperty(o.propName)&&(o.transform=n[o.propName]);return r}get outputs(){return Bc(this.componentDef.outputs,!1)}constructor(t,n){super(),this.componentDef=t,this.ngModule=n,this.componentType=t.type,this.selector=Dh(t.selectors),this.ngContentSelectors=t.ngContentSelectors?t.ngContentSelectors:[],this.isBoundToModule=!!n}create(t,n,r,o){let i=P(null);try{o=o||this.ngModule;let s=o instanceof pe?o:o?.injector;s&&this.componentDef.getStandaloneInjector!==null&&(s=this.componentDef.getStandaloneInjector(s)||s);let a=s?new xi(t,s):t,c=a.get(Qt,null);if(c===null)throw new g(407,!1);let u=a.get(hm,null),l=a.get(Zt,null),d={rendererFactory:c,sanitizer:u,inlineEffectRunner:null,changeDetectionScheduler:l},p=c.createRenderer(null,this.componentDef),f=this.componentDef.selectors[0][0]||"div",m=r?vE(p,r,this.componentDef.encapsulation,a):bl(p,f,pm(f)),w=512;this.componentDef.signals?w|=4096:this.componentDef.onPush||(w|=16);let V=null;m!==null&&(V=gs(m,a,!0));let B=Fl(0,null,null,1,0,null,null,null,null,null,null),ee=ys(null,B,null,w,null,null,d,p,a,null,V);ss(ee);let te,Le,Dt=null;try{let oe=this.componentDef,wt,ko=null;oe.findHostDirectiveDefs?(wt=[],ko=new Map,oe.findHostDirectiveDefs(oe,wt,ko),wt.push(oe)):wt=[oe];let gf=Em(ee,m);Dt=mm(gf,m,oe,wt,ee,d,p),Le=wu(B,lt),m&&Lm(p,oe,m,r),n!==void 0&&bm(Le,this.ngContentSelectors,n),te=Rm(Dt,oe,wt,ko,ee,[vm]),Ul(B,ee,null)}catch(oe){throw Dt!==null&&Pi(Dt),Pi(ee),oe}finally{as()}return new qi(this.componentType,te,hs(Le,ee),ee,Le)}finally{P(i)}}},qi=class extends Ui{constructor(t,n,r,o,i){super(),this.location=r,this._rootLView=o,this._tNode=i,this.previousInputValues=null,this.instance=n,this.hostView=this.changeDetectorRef=new Wt(o,void 0,!1),this.componentType=t}setInput(t,n){let r=this._tNode.inputs,o;if(r!==null&&(o=r[t])){if(this.previousInputValues??=new Map,this.previousInputValues.has(t)&&Object.is(this.previousInputValues.get(t),n))return;let i=this._rootLView;Cs(i[I],i,o,t,n),this.previousInputValues.set(t,n);let s=tt(this._tNode.index,i);Is(s,1)}}get injector(){return new ct(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(t){this.hostView.onDestroy(t)}};function Em(e,t){let n=e[I],r=lt;return e[r]=t,Ds(n,r,2,"#host",null)}function mm(e,t,n,r,o,i,s){let a=o[I];gm(r,e,t,s);let c=null;t!==null&&(c=gs(t,o[qt]));let u=i.rendererFactory.createRenderer(t,n),l=16;n.signals?l=4096:n.onPush&&(l=64);let d=ys(o,kl(n),null,l,o[e.index],e,i,u,null,null,c);return a.firstCreatePass&&Vi(a,e,r.length-1),ws(o,d),o[e.index]=d}function gm(e,t,n,r){for(let o of e)t.mergedAttrs=ts(t.mergedAttrs,o.hostAttrs);t.mergedAttrs!==null&&(zi(t,t.mergedAttrs,!0),n!==null&&Ml(r,n,t))}function Rm(e,t,n,r,o,i){let s=me(),a=o[I],c=Ee(s,o);jl(a,o,s,n,null,r);for(let l=0;l<n.length;l++){let d=s.directiveStart+l,p=Dn(o,a,d,s);Gt(p,o)}Vl(a,o,s),c&&Gt(c,o);let u=Dn(o,a,s.directiveStart+s.componentOffset,s);if(e[je]=o[je]=u,i!==null)for(let l of i)l(u,t);return Pl(a,s,o),u}function Lm(e,t,n,r){if(r)Li(e,n,["ng-version","18.2.13"]);else{let{attrs:o,classes:i}=wh(t.selectors[0]);o&&Li(e,n,o),i&&i.length>0&&xl(e,n,i.join(" "))}}function bm(e,t,n){let r=e.projection=[];for(let o=0;o<t.length;o++){let i=n[o];r.push(i!=null?Array.from(i):null)}}function vm(){let e=me();Gu(W()[I],e)}var ho=(()=>{class e{static{this.__NG_ELEMENT_ID__=Sm}}return e})();function Sm(){let e=me();return Dm(e,W())}var ym=ho,Kl=class extends ym{constructor(t,n,r){super(),this._lContainer=t,this._hostTNode=n,this._hostLView=r}get element(){return hs(this._hostTNode,this._hostLView)}get injector(){return new ct(this._hostTNode,this._hostLView)}get parentInjector(){let t=cs(this._hostTNode,this._hostLView);if(Zu(t)){let n=Br(t,this._hostLView),r=Vr(t),o=n[I].data[r+8];return new ct(o,n)}else return new ct(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(t){let n=$c(this._lContainer);return n!==null&&n[t]||null}get length(){return this._lContainer.length-he}createEmbeddedView(t,n,r){let o,i;typeof r=="number"?o=r:r!=null&&(o=r.index,i=r.injector);let s=Vc(this._lContainer,t.ssrId),a=t.createEmbeddedViewImpl(n||{},i,s);return this.insertImpl(a,o,jc(this._hostTNode,s)),a}createComponent(t,n,r,o,i){let s=t&&!$h(t),a;if(s)a=n;else{let m=n||{};a=m.index,r=m.injector,o=m.projectableNodes,i=m.environmentInjector||m.ngModuleRef}let c=s?t:new wn(ut(t)),u=r||this.parentInjector;if(!i&&c.ngModule==null){let w=(s?u:this.parentInjector).get(pe,null);w&&(i=w)}let l=ut(c.componentType??{}),d=Vc(this._lContainer,l?.id??null),p=d?.firstChild??null,f=c.create(u,o,p,i);return this.insertImpl(f.hostView,a,jc(this._hostTNode,d)),f}insert(t,n){return this.insertImpl(t,n,!0)}insertImpl(t,n,r){let o=t._lView;if(Wh(o)){let a=this.indexOf(t);if(a!==-1)this.detach(a);else{let c=o[X],u=new Kl(c,c[we],c[X]);u.detach(u.indexOf(t))}}let i=this._adjustIndex(n),s=this._lContainer;return QE(s,o,i,r),t.attachToViewContainerRef(),nu(Ei(s),i,t),t}move(t,n){return this.insert(t,n)}indexOf(t){let n=$c(this._lContainer);return n!==null?n.indexOf(t):-1}remove(t){let n=this._adjustIndex(t,-1),r=Fi(this._lContainer,n);r&&(Ar(Ei(this._lContainer),n),yl(r[I],r))}detach(t){let n=this._adjustIndex(t,-1),r=Fi(this._lContainer,n);return r&&Ar(Ei(this._lContainer),n)!=null?new Wt(r):null}_adjustIndex(t,n=0){return t??this.length+n}};function $c(e){return e[kr]}function Ei(e){return e[kr]||(e[kr]=[])}function Dm(e,t){let n,r=t[e.index];return He(r)?n=r:(n=zE(r,t,null,e),t[e.index]=n,ws(t,n)),Cm(n,t,e,r),new Kl(n,e,t)}function wm(e,t){let n=e[re],r=n.createComment(""),o=Ee(t,e),i=wl(n,o);return zr(n,i,r,cE(n,o),!1),r}var Cm=Im;function Im(e,t,n,r){if(e[dt])return;let o;n.type&8?o=Ve(r):o=wm(t,n),e[dt]=o}var Uc=new Set;function xs(e){Uc.has(e)||(Uc.add(e),performance?.mark?.("mark_feature_usage",{detail:{feature:e}}))}function Ms(e){let t=e.inputConfig,n={};for(let r in t)if(t.hasOwnProperty(r)){let o=t[r];Array.isArray(o)&&o[3]&&(n[r]=o[3])}e.inputTransforms=n}var Xe=class{},Cn=class{};var Gi=class extends Xe{constructor(t,n,r,o=!0){super(),this.ngModuleType=t,this._parent=n,this._bootstrapComponents=[],this.destroyCbs=[],this.componentFactoryResolver=new Wr(this);let i=du(t);this._bootstrapComponents=Ll(i.bootstrap),this._r3Injector=ol(t,n,[{provide:Xe,useValue:this},{provide:Yt,useValue:this.componentFactoryResolver},...r],ae(t),new Set(["environment"])),o&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let t=this._r3Injector;!t.destroyed&&t.destroy(),this.destroyCbs.forEach(n=>n()),this.destroyCbs=null}onDestroy(t){this.destroyCbs.push(t)}},Wi=class extends Cn{constructor(t){super(),this.moduleType=t}create(t){return new Gi(this.moduleType,t,[])}};var Zr=class extends Xe{constructor(t){super(),this.componentFactoryResolver=new Wr(this),this.instance=null;let n=new bn([...t.providers,{provide:Xe,useValue:this},{provide:Yt,useValue:this.componentFactoryResolver}],t.parent||os(),t.debugName,new Set(["environment"]));this.injector=n,t.runEnvironmentInitializers&&n.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(t){this.injector.onDestroy(t)}};function _s(e,t,n=null){return new Zr({providers:e,parent:t,debugName:n,runEnvironmentInitializers:!0}).injector}function xm(e,t,n){return e[t]=n}function Mm(e,t){return e[t]}function Jl(e,t,n){let r=e[t];return Object.is(r,n)?!1:(e[t]=n,!0)}function _m(e){return(e.flags&32)===32}var mn=function(e){return e[e.EarlyRead=0]="EarlyRead",e[e.Write=1]="Write",e[e.MixedReadWrite=2]="MixedReadWrite",e[e.Read=3]="Read",e}(mn||{}),Tm=(()=>{class e{constructor(){this.impl=null}execute(){this.impl?.execute()}static{this.\u0275prov=b({token:e,providedIn:"root",factory:()=>new e})}}return e})(),Hc=class e{constructor(){this.ngZone=h(H),this.scheduler=h(Zt),this.errorHandler=h(Be,{optional:!0}),this.sequences=new Set,this.deferredRegistrations=new Set,this.executing=!1}static{this.PHASES=[mn.EarlyRead,mn.Write,mn.MixedReadWrite,mn.Read]}execute(){this.executing=!0;for(let t of e.PHASES)for(let n of this.sequences)if(!(n.erroredOrDestroyed||!n.hooks[t]))try{n.pipelinedValue=this.ngZone.runOutsideAngular(()=>n.hooks[t](n.pipelinedValue))}catch(r){n.erroredOrDestroyed=!0,this.errorHandler?.handleError(r)}this.executing=!1;for(let t of this.sequences)t.afterRun(),t.once&&(this.sequences.delete(t),t.destroy());for(let t of this.deferredRegistrations)this.sequences.add(t);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear()}register(t){this.executing?this.deferredRegistrations.add(t):(this.sequences.add(t),this.scheduler.notify(6))}unregister(t){this.executing&&this.sequences.has(t)?(t.erroredOrDestroyed=!0,t.pipelinedValue=void 0,t.once=!0):(this.sequences.delete(t),this.deferredRegistrations.delete(t))}static{this.\u0275prov=b({token:e,providedIn:"root",factory:()=>new e})}};function Ts(e,t,n,r){let o=W(),i=Ou();if(Jl(o,i,t)){let s=Et(),a=Uu();BE(a,o,e,t,n,r)}return Ts}function As(e,t,n){let r=W(),o=Ou();if(Jl(r,o,t)){let i=Et(),s=Uu();IE(i,s,r,e,t,r[re],n,!1)}return As}function zc(e,t,n,r,o){let i=t.inputs,s=o?"class":"style";Cs(e,n,i[s],s,r)}function Am(e,t,n,r,o,i){let s=t.consts,a=Sc(s,o),c=Ds(t,e,2,r,a);return ME(t,n,c,Sc(s,i)),c.attrs!==null&&zi(c,c.attrs,!1),c.mergedAttrs!==null&&zi(c,c.mergedAttrs,!0),t.queries!==null&&t.queries.elementStart(t,c),c}function T(e,t,n,r){let o=W(),i=Et(),s=lt+e,a=o[re],c=i.firstCreatePass?Am(s,i,o,t,n,r):i.data[s],u=Nm(i,o,c,a,t,e);o[s]=u;let l=bu(c);return so(c,!0),Ml(a,u,c),!_m(c)&&zu()&&Cl(i,o,u,c),Yh()===0&&Gt(u,o),Qh(),l&&(RE(i,o,c),Pl(i,c,o)),r!==null&&LE(o,c),T}function N(){let e=me();Au()?np():(e=e.parent,so(e,!1));let t=e;Xh(t)&&ep(),Kh();let n=Et();return n.firstCreatePass&&(Gu(n,e),Lu(e)&&n.queries.elementEnd(e)),t.classesWithoutHost!=null&&hp(t)&&zc(n,t,W(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&pp(t)&&zc(n,t,W(),t.stylesWithoutHost,!1),N}function Oe(e,t,n,r){return T(e,t,n,r),N(),Oe}var Nm=(e,t,n,r,o,i)=>(qu(!0),bl(r,o,up()));var Yr="en-US";var Om=Yr;function Pm(e){typeof e=="string"&&(Om=e.toLowerCase().replace(/_/g,"-"))}var km=(e,t,n)=>{};function Ns(e,t,n,r){let o=W(),i=Et(),s=me();return jm(i,o,o[re],s,e,t,r),Ns}function Fm(e,t,n,r){let o=e.cleanup;if(o!=null)for(let i=0;i<o.length-1;i+=2){let s=o[i];if(s===n&&o[i+1]===r){let a=t[Pr],c=o[i+2];return a.length>c?a[c]:null}typeof s=="string"&&(i+=2)}return null}function jm(e,t,n,r,o,i,s){let a=bu(r),u=e.firstCreatePass&&GE(e),l=t[je],d=qE(t),p=!0;if(r.type&3||s){let w=Ee(r,t),V=s?s(w):w,B=d.length,ee=s?Le=>s(Ve(Le[r.index])):r.index,te=null;if(!s&&a&&(te=Fm(e,t,o,r.index)),te!==null){let Le=te.__ngLastListenerFn__||te;Le.__ngNextListenerFn__=i,te.__ngLastListenerFn__=i,p=!1}else{i=Gc(r,t,l,i),km(w,o,i);let Le=n.listen(V,o,i);d.push(i,Le),u&&u.push(o,ee,B,B+1)}}else i=Gc(r,t,l,i);let f=r.outputs,m;if(p&&f!==null&&(m=f[o])){let w=m.length;if(w)for(let V=0;V<w;V+=2){let B=m[V],ee=m[V+1],Dt=t[B][ee].subscribe(i),oe=d.length;d.push(i,Dt),u&&u.push(o,r.index,oe,-(oe+1))}}}function qc(e,t,n,r){let o=P(null);try{return xe(6,t,n),n(r)!==!1}catch(i){return $l(e,i),!1}finally{xe(7,t,n),P(o)}}function Gc(e,t,n,r){return function o(i){if(i===Function)return r;let s=e.componentOffset>-1?tt(e.index,t):t;Is(s,5);let a=qc(t,n,r,i),c=o.__ngNextListenerFn__;for(;c;)a=qc(t,n,c,i)&&a,c=c.__ngNextListenerFn__;return a}}function F(e,t=""){let n=W(),r=Et(),o=e+lt,i=r.firstCreatePass?Ds(r,o,1,t,null):r.data[o],s=Vm(r,n,i,t,e);n[o]=s,zu()&&Cl(r,n,s,i),so(i,!1)}var Vm=(e,t,n,r,o)=>(qu(!0),Xp(t[re],r));var Bm=(()=>{class e{constructor(n){this._injector=n,this.cachedInjectors=new Map}getOrCreateStandaloneInjector(n){if(!n.standalone)return null;if(!this.cachedInjectors.has(n)){let r=pu(!1,n.type),o=r.length>0?_s([r],this._injector,`Standalone[${n.type.name}]`):null;this.cachedInjectors.set(n,o)}return this.cachedInjectors.get(n)}ngOnDestroy(){try{for(let n of this.cachedInjectors.values())n!==null&&n.destroy()}finally{this.cachedInjectors.clear()}}static{this.\u0275prov=b({token:e,providedIn:"environment",factory:()=>new e(x(pe))})}}return e})();function nt(e){xs("NgStandalone"),e.getStandaloneInjector=t=>t.get(Bm).getOrCreateStandaloneInjector(e)}function Xl(e,t,n){let r=rp()+e,o=W();return o[r]===Ss?xm(o,r,n?t.call(n):t()):Mm(o,r)}var po=(()=>{class e{log(n){console.log(n)}warn(n){console.warn(n)}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"platform"})}}return e})();var ed=new y("");function Tn(e){return!!e&&typeof e.then=="function"}function td(e){return!!e&&typeof e.subscribe=="function"}var nd=new y(""),rd=(()=>{class e{constructor(){this.initialized=!1,this.done=!1,this.donePromise=new Promise((n,r)=>{this.resolve=n,this.reject=r}),this.appInits=h(nd,{optional:!0})??[]}runInitializers(){if(this.initialized)return;let n=[];for(let o of this.appInits){let i=o();if(Tn(i))n.push(i);else if(td(i)){let s=new Promise((a,c)=>{i.subscribe({complete:a,error:c})});n.push(s)}}let r=()=>{this.done=!0,this.resolve()};Promise.all(n).then(()=>{r()}).catch(o=>{this.reject(o)}),n.length===0&&r(),this.initialized=!0}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})(),Os=new y("");function $m(){Pa(()=>{throw new g(600,!1)})}function Um(e){return e.isBoundToModule}var Hm=10;function zm(e,t,n){try{let r=n();return Tn(r)?r.catch(o=>{throw t.runOutsideAngular(()=>e.handleError(o)),o}):r}catch(r){throw t.runOutsideAngular(()=>e.handleError(r)),r}}var gt=(()=>{class e{constructor(){this._bootstrapListeners=[],this._runningTick=!1,this._destroyed=!1,this._destroyListeners=[],this._views=[],this.internalErrorHandler=h(Pp),this.afterRenderManager=h(Tm),this.zonelessEnabled=h(lo),this.dirtyFlags=0,this.deferredDirtyFlags=0,this.externalTestViews=new Set,this.beforeRender=new K,this.afterTick=new K,this.componentTypes=[],this.components=[],this.isStable=h(Kt).hasPendingTasks.pipe(C(n=>!n)),this._injector=h(pe)}get allViews(){return[...this.externalTestViews.keys(),...this._views]}get destroyed(){return this._destroyed}whenStable(){let n;return new Promise(r=>{n=this.isStable.subscribe({next:o=>{o&&r()}})}).finally(()=>{n.unsubscribe()})}get injector(){return this._injector}bootstrap(n,r){let o=n instanceof Gr;if(!this._injector.get(rd).done){let p=!o&&lu(n),f=!1;throw new g(405,f)}let s;o?s=n:s=this._injector.get(Yt).resolveComponentFactory(n),this.componentTypes.push(s.componentType);let a=Um(s)?void 0:this._injector.get(Xe),c=r||s.selector,u=s.create(pt.NULL,[],c,a),l=u.location.nativeElement,d=u.injector.get(ed,null);return d?.registerApplication(l),u.onDestroy(()=>{this.detachView(u.hostView),xr(this.components,u),d?.unregisterApplication(l)}),this._loadComponent(u),u}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){if(this._runningTick)throw new g(101,!1);let n=P(null);try{this._runningTick=!0,this.synchronize()}catch(r){this.internalErrorHandler(r)}finally{this._runningTick=!1,P(n),this.afterTick.next()}}synchronize(){let n=null;this._injector.destroyed||(n=this._injector.get(Qt,null,{optional:!0})),this.dirtyFlags|=this.deferredDirtyFlags,this.deferredDirtyFlags=0;let r=0;for(;this.dirtyFlags!==0&&r++<Hm;)this.synchronizeOnce(n)}synchronizeOnce(n){if(this.dirtyFlags|=this.deferredDirtyFlags,this.deferredDirtyFlags=0,this.dirtyFlags&7){let r=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8,this.beforeRender.next(r);for(let{_lView:o,notifyErrorHandler:i}of this._views)qm(o,i,r,this.zonelessEnabled);if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&7)return}else n?.begin?.(),n?.end?.();this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:n})=>oo(n))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(n){let r=n;this._views.push(r),r.attachToAppRef(this)}detachView(n){let r=n;xr(this._views,r),r.detachFromAppRef()}_loadComponent(n){this.attachView(n.hostView),this.tick(),this.components.push(n);let r=this._injector.get(Os,[]);[...this._bootstrapListeners,...r].forEach(o=>o(n))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(n=>n()),this._views.slice().forEach(n=>n.destroy())}finally{this._destroyed=!0,this._views=[],this._bootstrapListeners=[],this._destroyListeners=[]}}onDestroy(n){return this._destroyListeners.push(n),()=>xr(this._destroyListeners,n)}destroy(){if(this._destroyed)throw new g(406,!1);let n=this._injector;n.destroy&&!n.destroyed&&n.destroy()}get viewCount(){return this._views.length}warnIfDestroyed(){}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})();function xr(e,t){let n=e.indexOf(t);n>-1&&e.splice(n,1)}function qm(e,t,n,r){if(!n&&!oo(e))return;ql(e,t,n&&!r?0:1)}var Zi=class{constructor(t,n){this.ngModuleFactory=t,this.componentFactories=n}},Ps=(()=>{class e{compileModuleSync(n){return new Wi(n)}compileModuleAsync(n){return Promise.resolve(this.compileModuleSync(n))}compileModuleAndAllComponentsSync(n){let r=this.compileModuleSync(n),o=du(n),i=Ll(o.declarations).reduce((s,a)=>{let c=ut(a);return c&&s.push(new wn(c)),s},[]);return new Zi(r,i)}compileModuleAndAllComponentsAsync(n){return Promise.resolve(this.compileModuleAndAllComponentsSync(n))}clearCache(){}clearCacheFor(n){}getModuleId(n){}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})();var Gm=(()=>{class e{constructor(){this.zone=h(H),this.changeDetectionScheduler=h(Zt),this.applicationRef=h(gt)}initialize(){this._onMicrotaskEmptySubscription||(this._onMicrotaskEmptySubscription=this.zone.onMicrotaskEmpty.subscribe({next:()=>{this.changeDetectionScheduler.runningTick||this.zone.run(()=>{this.applicationRef.tick()})}}))}ngOnDestroy(){this._onMicrotaskEmptySubscription?.unsubscribe()}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})(),Wm=new y("",{factory:()=>!1});function od({ngZoneFactory:e,ignoreChangesOutsideZone:t,scheduleInRootZone:n}){return e??=()=>new H($(E({},sd()),{scheduleInRootZone:n})),[{provide:H,useFactory:e},{provide:zt,multi:!0,useFactory:()=>{let r=h(Gm,{optional:!0});return()=>r.initialize()}},{provide:zt,multi:!0,useFactory:()=>{let r=h(Zm);return()=>{r.initialize()}}},t===!0?{provide:Yl,useValue:!0}:[],{provide:Ql,useValue:n??il}]}function id(e){let t=e?.ignoreChangesOutsideZone,n=e?.scheduleInRootZone,r=od({ngZoneFactory:()=>{let o=sd(e);return o.scheduleInRootZone=n,o.shouldCoalesceEventChangeDetection&&xs("NgZone_CoalesceEvent"),new H(o)},ignoreChangesOutsideZone:t,scheduleInRootZone:n});return Xr([{provide:Wm,useValue:!0},{provide:lo,useValue:!1},r])}function sd(e){return{enableLongStackTrace:!1,shouldCoalesceEventChangeDetection:e?.eventCoalescing??!1,shouldCoalesceRunChangeDetection:e?.runCoalescing??!1}}var Zm=(()=>{class e{constructor(){this.subscription=new U,this.initialized=!1,this.zone=h(H),this.pendingTasks=h(Kt)}initialize(){if(this.initialized)return;this.initialized=!0;let n=null;!this.zone.isStable&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(n=this.pendingTasks.add()),this.zone.runOutsideAngular(()=>{this.subscription.add(this.zone.onStable.subscribe(()=>{H.assertNotInAngularZone(),queueMicrotask(()=>{n!==null&&!this.zone.hasPendingMacrotasks&&!this.zone.hasPendingMicrotasks&&(this.pendingTasks.remove(n),n=null)})}))}),this.subscription.add(this.zone.onUnstable.subscribe(()=>{H.assertInAngularZone(),n??=this.pendingTasks.add()}))}ngOnDestroy(){this.subscription.unsubscribe()}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})();var Ym=(()=>{class e{constructor(){this.appRef=h(gt),this.taskService=h(Kt),this.ngZone=h(H),this.zonelessEnabled=h(lo),this.disableScheduling=h(Yl,{optional:!0})??!1,this.zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run,this.schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}],this.subscriptions=new U,this.angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Ur):null,this.scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(h(Ql,{optional:!0})??!1),this.cancelScheduledCallback=null,this.useMicrotaskScheduler=!1,this.runningTick=!1,this.pendingRenderTaskId=null,this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{this.runningTick||this.cleanup()})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()})),this.disableScheduling||=!this.zonelessEnabled&&(this.ngZone instanceof Oi||!this.zoneIsDefined)}notify(n){if(!this.zonelessEnabled&&n===5)return;switch(n){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 7:{this.appRef.deferredDirtyFlags|=8;break}case 9:case 8:case 6:case 10:default:this.appRef.dirtyFlags|=8}if(!this.shouldScheduleTick())return;let r=this.useMicrotaskScheduler?Mc:al;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>r(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>r(()=>this.tick()))}shouldScheduleTick(){return!(this.disableScheduling||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Ur+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let n=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(r){throw this.taskService.remove(n),r}finally{this.cleanup()}this.useMicrotaskScheduler=!0,Mc(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(n)})}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let n=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(n)}}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})();function Qm(){return typeof $localize<"u"&&$localize.locale||Yr}var ks=new y("",{providedIn:"root",factory:()=>h(ks,D.Optional|D.SkipSelf)||Qm()});var Yi=new y("");function Dr(e){return!e.moduleRef}function Km(e){let t=Dr(e)?e.r3Injector:e.moduleRef.injector,n=t.get(H);return n.run(()=>{Dr(e)?e.r3Injector.resolveInjectorInitializers():e.moduleRef.resolveInjectorInitializers();let r=t.get(Be,null),o;if(n.runOutsideAngular(()=>{o=n.onError.subscribe({next:i=>{r.handleError(i)}})}),Dr(e)){let i=()=>t.destroy(),s=e.platformInjector.get(Yi);s.add(i),t.onDestroy(()=>{o.unsubscribe(),s.delete(i)})}else{let i=()=>e.moduleRef.destroy(),s=e.platformInjector.get(Yi);s.add(i),e.moduleRef.onDestroy(()=>{xr(e.allPlatformModules,e.moduleRef),o.unsubscribe(),s.delete(i)})}return zm(r,n,()=>{let i=t.get(rd);return i.runInitializers(),i.donePromise.then(()=>{let s=t.get(ks,Yr);if(Pm(s||Yr),Dr(e)){let a=t.get(gt);return e.rootComponent!==void 0&&a.bootstrap(e.rootComponent),a}else return Jm(e.moduleRef,e.allPlatformModules),e.moduleRef})})})}function Jm(e,t){let n=e.injector.get(gt);if(e._bootstrapComponents.length>0)e._bootstrapComponents.forEach(r=>n.bootstrap(r));else if(e.instance.ngDoBootstrap)e.instance.ngDoBootstrap(n);else throw new g(-403,!1);t.push(e)}var Mr=null;function Xm(e=[],t){return pt.create({name:t,providers:[{provide:eo,useValue:"platform"},{provide:Yi,useValue:new Set([()=>Mr=null])},...e]})}function eg(e=[]){if(Mr)return Mr;let t=Xm(e);return Mr=t,$m(),tg(t),t}function tg(e){e.get(Es,null)?.forEach(n=>n())}var An=(()=>{class e{static{this.__NG_ELEMENT_ID__=ng}}return e})();function ng(e){return rg(me(),W(),(e&16)===16)}function rg(e,t,n){if(ro(e)&&!n){let r=tt(e.index,t);return new Wt(r,r)}else if(e.type&175){let r=t[Ae];return new Wt(r,t)}return null}function ad(e){try{let{rootComponent:t,appProviders:n,platformProviders:r}=e,o=eg(r),i=[od({}),{provide:Zt,useExisting:Ym},...n||[]],s=new Zr({providers:i,parent:o,debugName:"",runEnvironmentInitializers:!1});return Km({r3Injector:s.injector,platformInjector:o,rootComponent:t})}catch(t){return Promise.reject(t)}}function Nn(e){return typeof e=="boolean"?e:e!=null&&e!=="false"}var hd=null;function en(){return hd}function pd(e){hd??=e}var Eo=class{};var ge=new y(""),Ed=(()=>{class e{historyGo(n){throw new Error("")}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:()=>h(og),providedIn:"platform"})}}return e})();var og=(()=>{class e extends Ed{constructor(){super(),this._doc=h(ge),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return en().getBaseHref(this._doc)}onPopState(n){let r=en().getGlobalEventTarget(this._doc,"window");return r.addEventListener("popstate",n,!1),()=>r.removeEventListener("popstate",n)}onHashChange(n){let r=en().getGlobalEventTarget(this._doc,"window");return r.addEventListener("hashchange",n,!1),()=>r.removeEventListener("hashchange",n)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(n){this._location.pathname=n}pushState(n,r,o){this._history.pushState(n,r,o)}replaceState(n,r,o){this._history.replaceState(n,r,o)}forward(){this._history.forward()}back(){this._history.back()}historyGo(n=0){this._history.go(n)}getState(){return this._history.state}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:()=>new e,providedIn:"platform"})}}return e})();function md(e,t){if(e.length==0)return t;if(t.length==0)return e;let n=0;return e.endsWith("/")&&n++,t.startsWith("/")&&n++,n==2?e+t.substring(1):n==1?e+t:e+"/"+t}function cd(e){let t=e.match(/#|\?|$/),n=t&&t.index||e.length,r=n-(e[n-1]==="/"?1:0);return e.slice(0,r)+e.slice(n)}function Rt(e){return e&&e[0]!=="?"?"?"+e:e}var tn=(()=>{class e{historyGo(n){throw new Error("")}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:()=>h(gd),providedIn:"root"})}}return e})(),ig=new y(""),gd=(()=>{class e extends tn{constructor(n,r){super(),this._platformLocation=n,this._removeListenerFns=[],this._baseHref=r??this._platformLocation.getBaseHrefFromDOM()??h(ge).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(n){this._removeListenerFns.push(this._platformLocation.onPopState(n),this._platformLocation.onHashChange(n))}getBaseHref(){return this._baseHref}prepareExternalUrl(n){return md(this._baseHref,n)}path(n=!1){let r=this._platformLocation.pathname+Rt(this._platformLocation.search),o=this._platformLocation.hash;return o&&n?`${r}${o}`:r}pushState(n,r,o,i){let s=this.prepareExternalUrl(o+Rt(i));this._platformLocation.pushState(n,r,s)}replaceState(n,r,o,i){let s=this.prepareExternalUrl(o+Rt(i));this._platformLocation.replaceState(n,r,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(n=0){this._platformLocation.historyGo?.(n)}static{this.\u0275fac=function(r){return new(r||e)(x(Ed),x(ig,8))}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})();var On=(()=>{class e{constructor(n){this._subject=new J,this._urlChangeListeners=[],this._urlChangeSubscription=null,this._locationStrategy=n;let r=this._locationStrategy.getBaseHref();this._basePath=cg(cd(ud(r))),this._locationStrategy.onPopState(o=>{this._subject.emit({url:this.path(!0),pop:!0,state:o.state,type:o.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(n=!1){return this.normalize(this._locationStrategy.path(n))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(n,r=""){return this.path()==this.normalize(n+Rt(r))}normalize(n){return e.stripTrailingSlash(ag(this._basePath,ud(n)))}prepareExternalUrl(n){return n&&n[0]!=="/"&&(n="/"+n),this._locationStrategy.prepareExternalUrl(n)}go(n,r="",o=null){this._locationStrategy.pushState(o,"",n,r),this._notifyUrlChangeListeners(this.prepareExternalUrl(n+Rt(r)),o)}replaceState(n,r="",o=null){this._locationStrategy.replaceState(o,"",n,r),this._notifyUrlChangeListeners(this.prepareExternalUrl(n+Rt(r)),o)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(n=0){this._locationStrategy.historyGo?.(n)}onUrlChange(n){return this._urlChangeListeners.push(n),this._urlChangeSubscription??=this.subscribe(r=>{this._notifyUrlChangeListeners(r.url,r.state)}),()=>{let r=this._urlChangeListeners.indexOf(n);this._urlChangeListeners.splice(r,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(n="",r){this._urlChangeListeners.forEach(o=>o(n,r))}subscribe(n,r,o){return this._subject.subscribe({next:n,error:r,complete:o})}static{this.normalizeQueryParams=Rt}static{this.joinWithSlash=md}static{this.stripTrailingSlash=cd}static{this.\u0275fac=function(r){return new(r||e)(x(tn))}}static{this.\u0275prov=b({token:e,factory:()=>sg(),providedIn:"root"})}}return e})();function sg(){return new On(x(tn))}function ag(e,t){if(!e||!t.startsWith(e))return t;let n=t.substring(e.length);return n===""||["/",";","?","#"].includes(n[0])?n:t}function ud(e){return e.replace(/\/index.html$/,"")}function cg(e){if(new RegExp("^(https?:)?//").test(e)){let[,n]=e.split(/\/\/[^\/]+/);return n}return e}function Rd(e,t){t=encodeURIComponent(t);for(let n of e.split(";")){let r=n.indexOf("="),[o,i]=r==-1?[n,""]:[n.slice(0,r),n.slice(r+1)];if(o.trim()===t)return decodeURIComponent(i)}return null}var Ld="browser",ug="server";function Fs(e){return e===ug}var mo=class{};var Bs=class extends Eo{constructor(){super(...arguments),this.supportsDOMEvents=!0}},$s=class e extends Bs{static makeCurrent(){pd(new e)}onAndCancel(t,n,r){return t.addEventListener(n,r),()=>{t.removeEventListener(n,r)}}dispatchEvent(t,n){t.dispatchEvent(n)}remove(t){t.remove()}createElement(t,n){return n=n||this.getDefaultDocument(),n.createElement(t)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(t){return t.nodeType===Node.ELEMENT_NODE}isShadowRoot(t){return t instanceof DocumentFragment}getGlobalEventTarget(t,n){return n==="window"?window:n==="document"?t:n==="body"?t.body:null}getBaseHref(t){let n=fg();return n==null?null:hg(n)}resetBaseElement(){Pn=null}getUserAgent(){return window.navigator.userAgent}getCookie(t){return Rd(document.cookie,t)}},Pn=null;function fg(){return Pn=Pn||document.querySelector("base"),Pn?Pn.getAttribute("href"):null}function hg(e){return new URL(e,document.baseURI).pathname}var pg=(()=>{class e{build(){return new XMLHttpRequest}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac})}}return e})(),Us=new y(""),yd=(()=>{class e{constructor(n,r){this._zone=r,this._eventNameToPlugin=new Map,n.forEach(o=>{o.manager=this}),this._plugins=n.slice().reverse()}addEventListener(n,r,o){return this._findPluginFor(r).addEventListener(n,r,o)}getZone(){return this._zone}_findPluginFor(n){let r=this._eventNameToPlugin.get(n);if(r)return r;if(r=this._plugins.find(i=>i.supports(n)),!r)throw new g(5101,!1);return this._eventNameToPlugin.set(n,r),r}static{this.\u0275fac=function(r){return new(r||e)(x(Us),x(H))}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac})}}return e})(),go=class{constructor(t){this._doc=t}},js="ng-app-id",Dd=(()=>{class e{constructor(n,r,o,i={}){this.doc=n,this.appId=r,this.nonce=o,this.platformId=i,this.styleRef=new Map,this.hostNodes=new Set,this.styleNodesInDOM=this.collectServerRenderedStyles(),this.platformIsServer=Fs(i),this.resetHostNodes()}addStyles(n){for(let r of n)this.changeUsageCount(r,1)===1&&this.onStyleAdded(r)}removeStyles(n){for(let r of n)this.changeUsageCount(r,-1)<=0&&this.onStyleRemoved(r)}ngOnDestroy(){let n=this.styleNodesInDOM;n&&(n.forEach(r=>r.remove()),n.clear());for(let r of this.getAllStyles())this.onStyleRemoved(r);this.resetHostNodes()}addHost(n){this.hostNodes.add(n);for(let r of this.getAllStyles())this.addStyleToHost(n,r)}removeHost(n){this.hostNodes.delete(n)}getAllStyles(){return this.styleRef.keys()}onStyleAdded(n){for(let r of this.hostNodes)this.addStyleToHost(r,n)}onStyleRemoved(n){let r=this.styleRef;r.get(n)?.elements?.forEach(o=>o.remove()),r.delete(n)}collectServerRenderedStyles(){let n=this.doc.head?.querySelectorAll(`style[${js}="${this.appId}"]`);if(n?.length){let r=new Map;return n.forEach(o=>{o.textContent!=null&&r.set(o.textContent,o)}),r}return null}changeUsageCount(n,r){let o=this.styleRef;if(o.has(n)){let i=o.get(n);return i.usage+=r,i.usage}return o.set(n,{usage:r,elements:[]}),r}getStyleElement(n,r){let o=this.styleNodesInDOM,i=o?.get(r);if(i?.parentNode===n)return o.delete(r),i.removeAttribute(js),i;{let s=this.doc.createElement("style");return this.nonce&&s.setAttribute("nonce",this.nonce),s.textContent=r,this.platformIsServer&&s.setAttribute(js,this.appId),n.appendChild(s),s}}addStyleToHost(n,r){let o=this.getStyleElement(n,r),i=this.styleRef,s=i.get(r)?.elements;s?s.push(o):i.set(r,{elements:[o],usage:1})}resetHostNodes(){let n=this.hostNodes;n.clear(),n.add(this.doc.head)}static{this.\u0275fac=function(r){return new(r||e)(x(ge),x(ps),x(ms,8),x(Jt))}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac})}}return e})(),Vs={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},zs=/%COMP%/g,wd="%COMP%",Eg=`_nghost-${wd}`,mg=`_ngcontent-${wd}`,gg=!0,Rg=new y("",{providedIn:"root",factory:()=>gg});function Lg(e){return mg.replace(zs,e)}function bg(e){return Eg.replace(zs,e)}function Cd(e,t){return t.map(n=>n.replace(zs,e))}var bd=(()=>{class e{constructor(n,r,o,i,s,a,c,u=null){this.eventManager=n,this.sharedStylesHost=r,this.appId=o,this.removeStylesOnCompDestroy=i,this.doc=s,this.platformId=a,this.ngZone=c,this.nonce=u,this.rendererByCompId=new Map,this.platformIsServer=Fs(a),this.defaultRenderer=new kn(n,s,c,this.platformIsServer)}createRenderer(n,r){if(!n||!r)return this.defaultRenderer;this.platformIsServer&&r.encapsulation===_e.ShadowDom&&(r=$(E({},r),{encapsulation:_e.Emulated}));let o=this.getOrCreateRenderer(n,r);return o instanceof Ro?o.applyToHost(n):o instanceof Fn&&o.applyStyles(),o}getOrCreateRenderer(n,r){let o=this.rendererByCompId,i=o.get(r.id);if(!i){let s=this.doc,a=this.ngZone,c=this.eventManager,u=this.sharedStylesHost,l=this.removeStylesOnCompDestroy,d=this.platformIsServer;switch(r.encapsulation){case _e.Emulated:i=new Ro(c,u,r,this.appId,l,s,a,d);break;case _e.ShadowDom:return new Hs(c,u,n,r,s,a,this.nonce,d);default:i=new Fn(c,u,r,l,s,a,d);break}o.set(r.id,i)}return i}ngOnDestroy(){this.rendererByCompId.clear()}static{this.\u0275fac=function(r){return new(r||e)(x(yd),x(Dd),x(ps),x(Rg),x(ge),x(Jt),x(H),x(ms))}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac})}}return e})(),kn=class{constructor(t,n,r,o){this.eventManager=t,this.doc=n,this.ngZone=r,this.platformIsServer=o,this.data=Object.create(null),this.throwOnSyntheticProps=!0,this.destroyNode=null}destroy(){}createElement(t,n){return n?this.doc.createElementNS(Vs[n]||n,t):this.doc.createElement(t)}createComment(t){return this.doc.createComment(t)}createText(t){return this.doc.createTextNode(t)}appendChild(t,n){(vd(t)?t.content:t).appendChild(n)}insertBefore(t,n,r){t&&(vd(t)?t.content:t).insertBefore(n,r)}removeChild(t,n){n.remove()}selectRootElement(t,n){let r=typeof t=="string"?this.doc.querySelector(t):t;if(!r)throw new g(-5104,!1);return n||(r.textContent=""),r}parentNode(t){return t.parentNode}nextSibling(t){return t.nextSibling}setAttribute(t,n,r,o){if(o){n=o+":"+n;let i=Vs[o];i?t.setAttributeNS(i,n,r):t.setAttribute(n,r)}else t.setAttribute(n,r)}removeAttribute(t,n,r){if(r){let o=Vs[r];o?t.removeAttributeNS(o,n):t.removeAttribute(`${r}:${n}`)}else t.removeAttribute(n)}addClass(t,n){t.classList.add(n)}removeClass(t,n){t.classList.remove(n)}setStyle(t,n,r,o){o&(mt.DashCase|mt.Important)?t.style.setProperty(n,r,o&mt.Important?"important":""):t.style[n]=r}removeStyle(t,n,r){r&mt.DashCase?t.style.removeProperty(n):t.style[n]=""}setProperty(t,n,r){t!=null&&(t[n]=r)}setValue(t,n){t.nodeValue=n}listen(t,n,r){if(typeof t=="string"&&(t=en().getGlobalEventTarget(this.doc,t),!t))throw new Error(`Unsupported event target ${t} for event ${n}`);return this.eventManager.addEventListener(t,n,this.decoratePreventDefault(r))}decoratePreventDefault(t){return n=>{if(n==="__ngUnwrap__")return t;(this.platformIsServer?this.ngZone.runGuarded(()=>t(n)):t(n))===!1&&n.preventDefault()}}};function vd(e){return e.tagName==="TEMPLATE"&&e.content!==void 0}var Hs=class extends kn{constructor(t,n,r,o,i,s,a,c){super(t,i,s,c),this.sharedStylesHost=n,this.hostEl=r,this.shadowRoot=r.attachShadow({mode:"open"}),this.sharedStylesHost.addHost(this.shadowRoot);let u=Cd(o.id,o.styles);for(let l of u){let d=document.createElement("style");a&&d.setAttribute("nonce",a),d.textContent=l,this.shadowRoot.appendChild(d)}}nodeOrShadowRoot(t){return t===this.hostEl?this.shadowRoot:t}appendChild(t,n){return super.appendChild(this.nodeOrShadowRoot(t),n)}insertBefore(t,n,r){return super.insertBefore(this.nodeOrShadowRoot(t),n,r)}removeChild(t,n){return super.removeChild(null,n)}parentNode(t){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(t)))}destroy(){this.sharedStylesHost.removeHost(this.shadowRoot)}},Fn=class extends kn{constructor(t,n,r,o,i,s,a,c){super(t,i,s,a),this.sharedStylesHost=n,this.removeStylesOnCompDestroy=o,this.styles=c?Cd(c,r.styles):r.styles}applyStyles(){this.sharedStylesHost.addStyles(this.styles)}destroy(){this.removeStylesOnCompDestroy&&this.sharedStylesHost.removeStyles(this.styles)}},Ro=class extends Fn{constructor(t,n,r,o,i,s,a,c){let u=o+"-"+r.id;super(t,n,r,i,s,a,c,u),this.contentAttr=Lg(u),this.hostAttr=bg(u)}applyToHost(t){this.applyStyles(),this.setAttribute(t,this.hostAttr,"")}createElement(t,n){let r=super.createElement(t,n);return super.setAttribute(r,this.contentAttr,""),r}},vg=(()=>{class e extends go{constructor(n){super(n)}supports(n){return!0}addEventListener(n,r,o){return n.addEventListener(r,o,!1),()=>this.removeEventListener(n,r,o)}removeEventListener(n,r,o){return n.removeEventListener(r,o)}static{this.\u0275fac=function(r){return new(r||e)(x(ge))}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac})}}return e})(),Sd=["alt","control","meta","shift"],Sg={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},yg={alt:e=>e.altKey,control:e=>e.ctrlKey,meta:e=>e.metaKey,shift:e=>e.shiftKey},Dg=(()=>{class e extends go{constructor(n){super(n)}supports(n){return e.parseEventName(n)!=null}addEventListener(n,r,o){let i=e.parseEventName(r),s=e.eventCallback(i.fullKey,o,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>en().onAndCancel(n,i.domEventName,s))}static parseEventName(n){let r=n.toLowerCase().split("."),o=r.shift();if(r.length===0||!(o==="keydown"||o==="keyup"))return null;let i=e._normalizeKey(r.pop()),s="",a=r.indexOf("code");if(a>-1&&(r.splice(a,1),s="code."),Sd.forEach(u=>{let l=r.indexOf(u);l>-1&&(r.splice(l,1),s+=u+".")}),s+=i,r.length!=0||i.length===0)return null;let c={};return c.domEventName=o,c.fullKey=s,c}static matchEventFullKeyCode(n,r){let o=Sg[n.key]||n.key,i="";return r.indexOf("code.")>-1&&(o=n.code,i="code."),o==null||!o?!1:(o=o.toLowerCase(),o===" "?o="space":o==="."&&(o="dot"),Sd.forEach(s=>{if(s!==o){let a=yg[s];a(n)&&(i+=s+".")}}),i+=o,i===r)}static eventCallback(n,r,o){return i=>{e.matchEventFullKeyCode(i,n)&&o.runGuarded(()=>r(i))}}static _normalizeKey(n){return n==="esc"?"escape":n}static{this.\u0275fac=function(r){return new(r||e)(x(ge))}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac})}}return e})();function Id(e,t){return ad(E({rootComponent:e},wg(t)))}function wg(e){return{appProviders:[..._g,...e?.providers??[]],platformProviders:Mg}}function Cg(){$s.makeCurrent()}function Ig(){return new Be}function xg(){return pl(document),document}var Mg=[{provide:Jt,useValue:Ld},{provide:Es,useValue:Cg,multi:!0},{provide:ge,useFactory:xg,deps:[]}];var _g=[{provide:eo,useValue:"root"},{provide:Be,useFactory:Ig,deps:[]},{provide:Us,useClass:vg,multi:!0,deps:[ge,H,Jt]},{provide:Us,useClass:Dg,multi:!0,deps:[ge]},bd,Dd,yd,{provide:Qt,useExisting:bd},{provide:mo,useClass:pg,deps:[]},[]];var xd=(()=>{class e{constructor(n){this._doc=n}getTitle(){return this._doc.title}setTitle(n){this._doc.title=n||""}static{this.\u0275fac=function(r){return new(r||e)(x(ge))}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})();var S="primary",Xn=Symbol("RouteTitle"),Ys=class{constructor(t){this.params=t||{}}has(t){return Object.prototype.hasOwnProperty.call(this.params,t)}get(t){if(this.has(t)){let n=this.params[t];return Array.isArray(n)?n[0]:n}return null}getAll(t){if(this.has(t)){let n=this.params[t];return Array.isArray(n)?n:[n]}return[]}get keys(){return Object.keys(this.params)}};function cn(e){return new Ys(e)}function Ag(e,t,n){let r=n.path.split("/");if(r.length>e.length||n.pathMatch==="full"&&(t.hasChildren()||r.length<e.length))return null;let o={};for(let i=0;i<r.length;i++){let s=r[i],a=e[i];if(s[0]===":")o[s.substring(1)]=a;else if(s!==a.path)return null}return{consumed:e.slice(0,r.length),posParams:o}}function Ng(e,t){if(e.length!==t.length)return!1;for(let n=0;n<e.length;++n)if(!Pe(e[n],t[n]))return!1;return!0}function Pe(e,t){let n=e?Qs(e):void 0,r=t?Qs(t):void 0;if(!n||!r||n.length!=r.length)return!1;let o;for(let i=0;i<n.length;i++)if(o=n[i],!kd(e[o],t[o]))return!1;return!0}function Qs(e){return[...Object.keys(e),...Object.getOwnPropertySymbols(e)]}function kd(e,t){if(Array.isArray(e)&&Array.isArray(t)){if(e.length!==t.length)return!1;let n=[...e].sort(),r=[...t].sort();return n.every((o,i)=>r[i]===o)}else return e===t}function Fd(e){return e.length>0?e[e.length-1]:null}function ot(e){return ti(e)?e:Tn(e)?z(Promise.resolve(e)):v(e)}var Og={exact:Vd,subset:Bd},jd={exact:Pg,subset:kg,ignored:()=>!0};function Md(e,t,n){return Og[n.paths](e.root,t.root,n.matrixParams)&&jd[n.queryParams](e.queryParams,t.queryParams)&&!(n.fragment==="exact"&&e.fragment!==t.fragment)}function Pg(e,t){return Pe(e,t)}function Vd(e,t,n){if(!bt(e.segments,t.segments)||!vo(e.segments,t.segments,n)||e.numberOfChildren!==t.numberOfChildren)return!1;for(let r in t.children)if(!e.children[r]||!Vd(e.children[r],t.children[r],n))return!1;return!0}function kg(e,t){return Object.keys(t).length<=Object.keys(e).length&&Object.keys(t).every(n=>kd(e[n],t[n]))}function Bd(e,t,n){return $d(e,t,t.segments,n)}function $d(e,t,n,r){if(e.segments.length>n.length){let o=e.segments.slice(0,n.length);return!(!bt(o,n)||t.hasChildren()||!vo(o,n,r))}else if(e.segments.length===n.length){if(!bt(e.segments,n)||!vo(e.segments,n,r))return!1;for(let o in t.children)if(!e.children[o]||!Bd(e.children[o],t.children[o],r))return!1;return!0}else{let o=n.slice(0,e.segments.length),i=n.slice(e.segments.length);return!bt(e.segments,o)||!vo(e.segments,o,r)||!e.children[S]?!1:$d(e.children[S],t,i,r)}}function vo(e,t,n){return t.every((r,o)=>jd[n](e[o].parameters,r.parameters))}var qe=class{constructor(t=new O([],{}),n={},r=null){this.root=t,this.queryParams=n,this.fragment=r}get queryParamMap(){return this._queryParamMap??=cn(this.queryParams),this._queryParamMap}toString(){return Vg.serialize(this)}},O=class{constructor(t,n){this.segments=t,this.children=n,this.parent=null,Object.values(n).forEach(r=>r.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return So(this)}},Lt=class{constructor(t,n){this.path=t,this.parameters=n}get parameterMap(){return this._parameterMap??=cn(this.parameters),this._parameterMap}toString(){return Hd(this)}};function Fg(e,t){return bt(e,t)&&e.every((n,r)=>Pe(n.parameters,t[r].parameters))}function bt(e,t){return e.length!==t.length?!1:e.every((n,r)=>n.path===t[r].path)}function jg(e,t){let n=[];return Object.entries(e.children).forEach(([r,o])=>{r===S&&(n=n.concat(t(o,r)))}),Object.entries(e.children).forEach(([r,o])=>{r!==S&&(n=n.concat(t(o,r)))}),n}var Sa=(()=>{class e{static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:()=>new zn,providedIn:"root"})}}return e})(),zn=class{parse(t){let n=new Js(t);return new qe(n.parseRootSegment(),n.parseQueryParams(),n.parseFragment())}serialize(t){let n=`/${jn(t.root,!0)}`,r=Ug(t.queryParams),o=typeof t.fragment=="string"?`#${Bg(t.fragment)}`:"";return`${n}${r}${o}`}},Vg=new zn;function So(e){return e.segments.map(t=>Hd(t)).join("/")}function jn(e,t){if(!e.hasChildren())return So(e);if(t){let n=e.children[S]?jn(e.children[S],!1):"",r=[];return Object.entries(e.children).forEach(([o,i])=>{o!==S&&r.push(`${o}:${jn(i,!1)}`)}),r.length>0?`${n}(${r.join("//")})`:n}else{let n=jg(e,(r,o)=>o===S?[jn(e.children[S],!1)]:[`${o}:${jn(r,!1)}`]);return Object.keys(e.children).length===1&&e.children[S]!=null?`${So(e)}/${n[0]}`:`${So(e)}/(${n.join("//")})`}}function Ud(e){return encodeURIComponent(e).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Lo(e){return Ud(e).replace(/%3B/gi,";")}function Bg(e){return encodeURI(e)}function Ks(e){return Ud(e).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function yo(e){return decodeURIComponent(e)}function _d(e){return yo(e.replace(/\+/g,"%20"))}function Hd(e){return`${Ks(e.path)}${$g(e.parameters)}`}function $g(e){return Object.entries(e).map(([t,n])=>`;${Ks(t)}=${Ks(n)}`).join("")}function Ug(e){let t=Object.entries(e).map(([n,r])=>Array.isArray(r)?r.map(o=>`${Lo(n)}=${Lo(o)}`).join("&"):`${Lo(n)}=${Lo(r)}`).filter(n=>n);return t.length?`?${t.join("&")}`:""}var Hg=/^[^\/()?;#]+/;function qs(e){let t=e.match(Hg);return t?t[0]:""}var zg=/^[^\/()?;=#]+/;function qg(e){let t=e.match(zg);return t?t[0]:""}var Gg=/^[^=?&#]+/;function Wg(e){let t=e.match(Gg);return t?t[0]:""}var Zg=/^[^&#]+/;function Yg(e){let t=e.match(Zg);return t?t[0]:""}var Js=class{constructor(t){this.url=t,this.remaining=t}parseRootSegment(){return this.consumeOptional("/"),this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new O([],{}):new O([],this.parseChildren())}parseQueryParams(){let t={};if(this.consumeOptional("?"))do this.parseQueryParam(t);while(this.consumeOptional("&"));return t}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(){if(this.remaining==="")return{};this.consumeOptional("/");let t=[];for(this.peekStartsWith("(")||t.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),t.push(this.parseSegment());let n={};this.peekStartsWith("/(")&&(this.capture("/"),n=this.parseParens(!0));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1)),(t.length>0||Object.keys(n).length>0)&&(r[S]=new O(t,n)),r}parseSegment(){let t=qs(this.remaining);if(t===""&&this.peekStartsWith(";"))throw new g(4009,!1);return this.capture(t),new Lt(yo(t),this.parseMatrixParams())}parseMatrixParams(){let t={};for(;this.consumeOptional(";");)this.parseParam(t);return t}parseParam(t){let n=qg(this.remaining);if(!n)return;this.capture(n);let r="";if(this.consumeOptional("=")){let o=qs(this.remaining);o&&(r=o,this.capture(r))}t[yo(n)]=yo(r)}parseQueryParam(t){let n=Wg(this.remaining);if(!n)return;this.capture(n);let r="";if(this.consumeOptional("=")){let s=Yg(this.remaining);s&&(r=s,this.capture(r))}let o=_d(n),i=_d(r);if(t.hasOwnProperty(o)){let s=t[o];Array.isArray(s)||(s=[s],t[o]=s),s.push(i)}else t[o]=i}parseParens(t){let n={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=qs(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new g(4010,!1);let i;r.indexOf(":")>-1?(i=r.slice(0,r.indexOf(":")),this.capture(i),this.capture(":")):t&&(i=S);let s=this.parseChildren();n[i]=Object.keys(s).length===1?s[S]:new O([],s),this.consumeOptional("//")}return n}peekStartsWith(t){return this.remaining.startsWith(t)}consumeOptional(t){return this.peekStartsWith(t)?(this.remaining=this.remaining.substring(t.length),!0):!1}capture(t){if(!this.consumeOptional(t))throw new g(4011,!1)}};function zd(e){return e.segments.length>0?new O([],{[S]:e}):e}function qd(e){let t={};for(let[r,o]of Object.entries(e.children)){let i=qd(o);if(r===S&&i.segments.length===0&&i.hasChildren())for(let[s,a]of Object.entries(i.children))t[s]=a;else(i.segments.length>0||i.hasChildren())&&(t[r]=i)}let n=new O(e.segments,t);return Qg(n)}function Qg(e){if(e.numberOfChildren===1&&e.children[S]){let t=e.children[S];return new O(e.segments.concat(t.segments),t.children)}return e}function vt(e){return e instanceof qe}function Kg(e,t,n=null,r=null){let o=Gd(e);return Wd(o,t,n,r)}function Gd(e){let t;function n(i){let s={};for(let c of i.children){let u=n(c);s[c.outlet]=u}let a=new O(i.url,s);return i===e&&(t=a),a}let r=n(e.root),o=zd(r);return t??o}function Wd(e,t,n,r){let o=e;for(;o.parent;)o=o.parent;if(t.length===0)return Gs(o,o,o,n,r);let i=Jg(t);if(i.toRoot())return Gs(o,o,new O([],{}),n,r);let s=Xg(i,o,e),a=s.processChildren?$n(s.segmentGroup,s.index,i.commands):Yd(s.segmentGroup,s.index,i.commands);return Gs(o,s.segmentGroup,a,n,r)}function Do(e){return typeof e=="object"&&e!=null&&!e.outlets&&!e.segmentPath}function qn(e){return typeof e=="object"&&e!=null&&e.outlets}function Gs(e,t,n,r,o){let i={};r&&Object.entries(r).forEach(([c,u])=>{i[c]=Array.isArray(u)?u.map(l=>`${l}`):`${u}`});let s;e===t?s=n:s=Zd(e,t,n);let a=zd(qd(s));return new qe(a,i,o)}function Zd(e,t,n){let r={};return Object.entries(e.children).forEach(([o,i])=>{i===t?r[o]=n:r[o]=Zd(i,t,n)}),new O(e.segments,r)}var wo=class{constructor(t,n,r){if(this.isAbsolute=t,this.numberOfDoubleDots=n,this.commands=r,t&&r.length>0&&Do(r[0]))throw new g(4003,!1);let o=r.find(qn);if(o&&o!==Fd(r))throw new g(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function Jg(e){if(typeof e[0]=="string"&&e.length===1&&e[0]==="/")return new wo(!0,0,e);let t=0,n=!1,r=e.reduce((o,i,s)=>{if(typeof i=="object"&&i!=null){if(i.outlets){let a={};return Object.entries(i.outlets).forEach(([c,u])=>{a[c]=typeof u=="string"?u.split("/"):u}),[...o,{outlets:a}]}if(i.segmentPath)return[...o,i.segmentPath]}return typeof i!="string"?[...o,i]:s===0?(i.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?n=!0:a===".."?t++:a!=""&&o.push(a))}),o):[...o,i]},[]);return new wo(n,t,r)}var on=class{constructor(t,n,r){this.segmentGroup=t,this.processChildren=n,this.index=r}};function Xg(e,t,n){if(e.isAbsolute)return new on(t,!0,0);if(!n)return new on(t,!1,NaN);if(n.parent===null)return new on(n,!0,0);let r=Do(e.commands[0])?0:1,o=n.segments.length-1+r;return eR(n,o,e.numberOfDoubleDots)}function eR(e,t,n){let r=e,o=t,i=n;for(;i>o;){if(i-=o,r=r.parent,!r)throw new g(4005,!1);o=r.segments.length}return new on(r,!1,o-i)}function tR(e){return qn(e[0])?e[0].outlets:{[S]:e}}function Yd(e,t,n){if(e??=new O([],{}),e.segments.length===0&&e.hasChildren())return $n(e,t,n);let r=nR(e,t,n),o=n.slice(r.commandIndex);if(r.match&&r.pathIndex<e.segments.length){let i=new O(e.segments.slice(0,r.pathIndex),{});return i.children[S]=new O(e.segments.slice(r.pathIndex),e.children),$n(i,0,o)}else return r.match&&o.length===0?new O(e.segments,{}):r.match&&!e.hasChildren()?Xs(e,t,n):r.match?$n(e,0,o):Xs(e,t,n)}function $n(e,t,n){if(n.length===0)return new O(e.segments,{});{let r=tR(n),o={};if(Object.keys(r).some(i=>i!==S)&&e.children[S]&&e.numberOfChildren===1&&e.children[S].segments.length===0){let i=$n(e.children[S],t,n);return new O(e.segments,i.children)}return Object.entries(r).forEach(([i,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(o[i]=Yd(e.children[i],t,s))}),Object.entries(e.children).forEach(([i,s])=>{r[i]===void 0&&(o[i]=s)}),new O(e.segments,o)}}function nR(e,t,n){let r=0,o=t,i={match:!1,pathIndex:0,commandIndex:0};for(;o<e.segments.length;){if(r>=n.length)return i;let s=e.segments[o],a=n[r];if(qn(a))break;let c=`${a}`,u=r<n.length-1?n[r+1]:null;if(o>0&&c===void 0)break;if(c&&u&&typeof u=="object"&&u.outlets===void 0){if(!Ad(c,u,s))return i;r+=2}else{if(!Ad(c,{},s))return i;r++}o++}return{match:!0,pathIndex:o,commandIndex:r}}function Xs(e,t,n){let r=e.segments.slice(0,t),o=0;for(;o<n.length;){let i=n[o];if(qn(i)){let c=rR(i.outlets);return new O(r,c)}if(o===0&&Do(n[0])){let c=e.segments[t];r.push(new Lt(c.path,Td(n[0]))),o++;continue}let s=qn(i)?i.outlets[S]:`${i}`,a=o<n.length-1?n[o+1]:null;s&&a&&Do(a)?(r.push(new Lt(s,Td(a))),o+=2):(r.push(new Lt(s,{})),o++)}return new O(r,{})}function rR(e){let t={};return Object.entries(e).forEach(([n,r])=>{typeof r=="string"&&(r=[r]),r!==null&&(t[n]=Xs(new O([],{}),0,r))}),t}function Td(e){let t={};return Object.entries(e).forEach(([n,r])=>t[n]=`${r}`),t}function Ad(e,t,n){return e==n.path&&Pe(t,n.parameters)}var Un="imperative",Q=function(e){return e[e.NavigationStart=0]="NavigationStart",e[e.NavigationEnd=1]="NavigationEnd",e[e.NavigationCancel=2]="NavigationCancel",e[e.NavigationError=3]="NavigationError",e[e.RoutesRecognized=4]="RoutesRecognized",e[e.ResolveStart=5]="ResolveStart",e[e.ResolveEnd=6]="ResolveEnd",e[e.GuardsCheckStart=7]="GuardsCheckStart",e[e.GuardsCheckEnd=8]="GuardsCheckEnd",e[e.RouteConfigLoadStart=9]="RouteConfigLoadStart",e[e.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",e[e.ChildActivationStart=11]="ChildActivationStart",e[e.ChildActivationEnd=12]="ChildActivationEnd",e[e.ActivationStart=13]="ActivationStart",e[e.ActivationEnd=14]="ActivationEnd",e[e.Scroll=15]="Scroll",e[e.NavigationSkipped=16]="NavigationSkipped",e}(Q||{}),Re=class{constructor(t,n){this.id=t,this.url=n}},Gn=class extends Re{constructor(t,n,r="imperative",o=null){super(t,n),this.type=Q.NavigationStart,this.navigationTrigger=r,this.restoredState=o}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},rt=class extends Re{constructor(t,n,r){super(t,n),this.urlAfterRedirects=r,this.type=Q.NavigationEnd}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},ue=function(e){return e[e.Redirect=0]="Redirect",e[e.SupersededByNewNavigation=1]="SupersededByNewNavigation",e[e.NoDataFromResolver=2]="NoDataFromResolver",e[e.GuardRejected=3]="GuardRejected",e}(ue||{}),ea=function(e){return e[e.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",e[e.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",e}(ea||{}),ze=class extends Re{constructor(t,n,r,o){super(t,n),this.reason=r,this.code=o,this.type=Q.NavigationCancel}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}},St=class extends Re{constructor(t,n,r,o){super(t,n),this.reason=r,this.code=o,this.type=Q.NavigationSkipped}},Wn=class extends Re{constructor(t,n,r,o){super(t,n),this.error=r,this.target=o,this.type=Q.NavigationError}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Co=class extends Re{constructor(t,n,r,o){super(t,n),this.urlAfterRedirects=r,this.state=o,this.type=Q.RoutesRecognized}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},ta=class extends Re{constructor(t,n,r,o){super(t,n),this.urlAfterRedirects=r,this.state=o,this.type=Q.GuardsCheckStart}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},na=class extends Re{constructor(t,n,r,o,i){super(t,n),this.urlAfterRedirects=r,this.state=o,this.shouldActivate=i,this.type=Q.GuardsCheckEnd}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},ra=class extends Re{constructor(t,n,r,o){super(t,n),this.urlAfterRedirects=r,this.state=o,this.type=Q.ResolveStart}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},oa=class extends Re{constructor(t,n,r,o){super(t,n),this.urlAfterRedirects=r,this.state=o,this.type=Q.ResolveEnd}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},ia=class{constructor(t){this.route=t,this.type=Q.RouteConfigLoadStart}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},sa=class{constructor(t){this.route=t,this.type=Q.RouteConfigLoadEnd}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},aa=class{constructor(t){this.snapshot=t,this.type=Q.ChildActivationStart}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},ca=class{constructor(t){this.snapshot=t,this.type=Q.ChildActivationEnd}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},ua=class{constructor(t){this.snapshot=t,this.type=Q.ActivationStart}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},la=class{constructor(t){this.snapshot=t,this.type=Q.ActivationEnd}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Zn=class{},un=class{constructor(t,n){this.url=t,this.navigationBehaviorOptions=n}};function oR(e,t){return e.providers&&!e._injector&&(e._injector=_s(e.providers,t,`Route: ${e.path}`)),e._injector??t}function Ce(e){return e.outlet||S}function iR(e,t){let n=e.filter(r=>Ce(r)===t);return n.push(...e.filter(r=>Ce(r)!==t)),n}function er(e){if(!e)return null;if(e.routeConfig?._injector)return e.routeConfig._injector;for(let t=e.parent;t;t=t.parent){let n=t.routeConfig;if(n?._loadedInjector)return n._loadedInjector;if(n?._injector)return n._injector}return null}var da=class{get injector(){return er(this.route?.snapshot)??this.rootInjector}set injector(t){}constructor(t){this.rootInjector=t,this.outlet=null,this.route=null,this.children=new No(this.rootInjector),this.attachRef=null}},No=(()=>{class e{constructor(n){this.rootInjector=n,this.contexts=new Map}onChildOutletCreated(n,r){let o=this.getOrCreateContext(n);o.outlet=r,this.contexts.set(n,o)}onChildOutletDestroyed(n){let r=this.getContext(n);r&&(r.outlet=null,r.attachRef=null)}onOutletDeactivated(){let n=this.contexts;return this.contexts=new Map,n}onOutletReAttached(n){this.contexts=n}getOrCreateContext(n){let r=this.getContext(n);return r||(r=new da(this.rootInjector),this.contexts.set(n,r)),r}getContext(n){return this.contexts.get(n)||null}static{this.\u0275fac=function(r){return new(r||e)(x(pe))}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})(),Io=class{constructor(t){this._root=t}get root(){return this._root.value}parent(t){let n=this.pathFromRoot(t);return n.length>1?n[n.length-2]:null}children(t){let n=fa(t,this._root);return n?n.children.map(r=>r.value):[]}firstChild(t){let n=fa(t,this._root);return n&&n.children.length>0?n.children[0].value:null}siblings(t){let n=ha(t,this._root);return n.length<2?[]:n[n.length-2].children.map(o=>o.value).filter(o=>o!==t)}pathFromRoot(t){return ha(t,this._root).map(n=>n.value)}};function fa(e,t){if(e===t.value)return t;for(let n of t.children){let r=fa(e,n);if(r)return r}return null}function ha(e,t){if(e===t.value)return[t];for(let n of t.children){let r=ha(e,n);if(r.length)return r.unshift(t),r}return[]}var ce=class{constructor(t,n){this.value=t,this.children=n}toString(){return`TreeNode(${this.value})`}};function rn(e){let t={};return e&&e.children.forEach(n=>t[n.value.outlet]=n),t}var xo=class extends Io{constructor(t,n){super(t),this.snapshot=n,ya(this,t)}toString(){return this.snapshot.toString()}};function Qd(e){let t=sR(e),n=new Z([new Lt("",{})]),r=new Z({}),o=new Z({}),i=new Z({}),s=new Z(""),a=new yt(n,r,i,s,o,S,e,t.root);return a.snapshot=t.root,new xo(new ce(a,[]),t)}function sR(e){let t={},n={},r={},o="",i=new sn([],t,r,o,n,S,e,null,{});return new _o("",new ce(i,[]))}var yt=class{constructor(t,n,r,o,i,s,a,c){this.urlSubject=t,this.paramsSubject=n,this.queryParamsSubject=r,this.fragmentSubject=o,this.dataSubject=i,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(C(u=>u[Xn]))??v(void 0),this.url=t,this.params=n,this.queryParams=r,this.fragment=o,this.data=i}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(C(t=>cn(t))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(C(t=>cn(t))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function Mo(e,t,n="emptyOnly"){let r,{routeConfig:o}=e;return t!==null&&(n==="always"||o?.path===""||!t.component&&!t.routeConfig?.loadComponent)?r={params:E(E({},t.params),e.params),data:E(E({},t.data),e.data),resolve:E(E(E(E({},e.data),t.data),o?.data),e._resolvedData)}:r={params:E({},e.params),data:E({},e.data),resolve:E(E({},e.data),e._resolvedData??{})},o&&Jd(o)&&(r.resolve[Xn]=o.title),r}var sn=class{get title(){return this.data?.[Xn]}constructor(t,n,r,o,i,s,a,c,u){this.url=t,this.params=n,this.queryParams=r,this.fragment=o,this.data=i,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=cn(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=cn(this.queryParams),this._queryParamMap}toString(){let t=this.url.map(r=>r.toString()).join("/"),n=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${t}', path:'${n}')`}},_o=class extends Io{constructor(t,n){super(n),this.url=t,ya(this,n)}toString(){return Kd(this._root)}};function ya(e,t){t.value._routerState=e,t.children.forEach(n=>ya(e,n))}function Kd(e){let t=e.children.length>0?` { ${e.children.map(Kd).join(", ")} } `:"";return`${e.value}${t}`}function Ws(e){if(e.snapshot){let t=e.snapshot,n=e._futureSnapshot;e.snapshot=n,Pe(t.queryParams,n.queryParams)||e.queryParamsSubject.next(n.queryParams),t.fragment!==n.fragment&&e.fragmentSubject.next(n.fragment),Pe(t.params,n.params)||e.paramsSubject.next(n.params),Ng(t.url,n.url)||e.urlSubject.next(n.url),Pe(t.data,n.data)||e.dataSubject.next(n.data)}else e.snapshot=e._futureSnapshot,e.dataSubject.next(e._futureSnapshot.data)}function pa(e,t){let n=Pe(e.params,t.params)&&Fg(e.url,t.url),r=!e.parent!=!t.parent;return n&&!r&&(!e.parent||pa(e.parent,t.parent))}function Jd(e){return typeof e.title=="string"||e.title===null}var Da=(()=>{class e{constructor(){this.activated=null,this._activatedRoute=null,this.name=S,this.activateEvents=new J,this.deactivateEvents=new J,this.attachEvents=new J,this.detachEvents=new J,this.parentContexts=h(No),this.location=h(ho),this.changeDetector=h(An),this.inputBinder=h(wa,{optional:!0}),this.supportsBindingToComponentInputs=!0}get activatedComponentRef(){return this.activated}ngOnChanges(n){if(n.name){let{firstChange:r,previousValue:o}=n.name;if(r)return;this.isTrackedInParentContexts(o)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(o)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(n){return this.parentContexts.getContext(n)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let n=this.parentContexts.getContext(this.name);n?.route&&(n.attachRef?this.attach(n.attachRef,n.route):this.activateWith(n.route,n.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new g(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new g(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new g(4012,!1);this.location.detach();let n=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(n.instance),n}attach(n,r){this.activated=n,this._activatedRoute=r,this.location.insert(n.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(n.instance)}deactivate(){if(this.activated){let n=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(n)}}activateWith(n,r){if(this.isActivated)throw new g(4013,!1);this._activatedRoute=n;let o=this.location,s=n.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new Ea(n,a,o.injector);this.activated=o.createComponent(s,{index:o.length,injector:c,environmentInjector:r}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275dir=Jr({type:e,selectors:[["router-outlet"]],inputs:{name:"name"},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],standalone:!0,features:[Mn]})}}return e})(),Ea=class e{__ngOutletInjector(t){return new e(this.route,this.childContexts,t)}constructor(t,n,r){this.route=t,this.childContexts=n,this.parent=r}get(t,n){return t===yt?this.route:t===No?this.childContexts:this.parent.get(t,n)}},wa=new y("");function aR(e,t,n){let r=Yn(e,t._root,n?n._root:void 0);return new xo(r,t)}function Yn(e,t,n){if(n&&e.shouldReuseRoute(t.value,n.value.snapshot)){let r=n.value;r._futureSnapshot=t.value;let o=cR(e,t,n);return new ce(r,o)}else{if(e.shouldAttach(t.value)){let i=e.retrieve(t.value);if(i!==null){let s=i.route;return s.value._futureSnapshot=t.value,s.children=t.children.map(a=>Yn(e,a)),s}}let r=uR(t.value),o=t.children.map(i=>Yn(e,i));return new ce(r,o)}}function cR(e,t,n){return t.children.map(r=>{for(let o of n.children)if(e.shouldReuseRoute(r.value,o.value.snapshot))return Yn(e,r,o);return Yn(e,r)})}function uR(e){return new yt(new Z(e.url),new Z(e.params),new Z(e.queryParams),new Z(e.fragment),new Z(e.data),e.outlet,e.component,e)}var Qn=class{constructor(t,n){this.redirectTo=t,this.navigationBehaviorOptions=n}},Xd="ngNavigationCancelingError";function To(e,t){let{redirectTo:n,navigationBehaviorOptions:r}=vt(t)?{redirectTo:t,navigationBehaviorOptions:void 0}:t,o=ef(!1,ue.Redirect);return o.url=n,o.navigationBehaviorOptions=r,o}function ef(e,t){let n=new Error(`NavigationCancelingError: ${e||""}`);return n[Xd]=!0,n.cancellationCode=t,n}function lR(e){return tf(e)&&vt(e.url)}function tf(e){return!!e&&e[Xd]}var dR=(e,t,n,r)=>C(o=>(new ma(t,o.targetRouterState,o.currentRouterState,n,r).activate(e),o)),ma=class{constructor(t,n,r,o,i){this.routeReuseStrategy=t,this.futureState=n,this.currState=r,this.forwardEvent=o,this.inputBindingEnabled=i}activate(t){let n=this.futureState._root,r=this.currState?this.currState._root:null;this.deactivateChildRoutes(n,r,t),Ws(this.futureState.root),this.activateChildRoutes(n,r,t)}deactivateChildRoutes(t,n,r){let o=rn(n);t.children.forEach(i=>{let s=i.value.outlet;this.deactivateRoutes(i,o[s],r),delete o[s]}),Object.values(o).forEach(i=>{this.deactivateRouteAndItsChildren(i,r)})}deactivateRoutes(t,n,r){let o=t.value,i=n?n.value:null;if(o===i)if(o.component){let s=r.getContext(o.outlet);s&&this.deactivateChildRoutes(t,n,s.children)}else this.deactivateChildRoutes(t,n,r);else i&&this.deactivateRouteAndItsChildren(n,r)}deactivateRouteAndItsChildren(t,n){t.value.component&&this.routeReuseStrategy.shouldDetach(t.value.snapshot)?this.detachAndStoreRouteSubtree(t,n):this.deactivateRouteAndOutlet(t,n)}detachAndStoreRouteSubtree(t,n){let r=n.getContext(t.value.outlet),o=r&&t.value.component?r.children:n,i=rn(t);for(let s of Object.values(i))this.deactivateRouteAndItsChildren(s,o);if(r&&r.outlet){let s=r.outlet.detach(),a=r.children.onOutletDeactivated();this.routeReuseStrategy.store(t.value.snapshot,{componentRef:s,route:t,contexts:a})}}deactivateRouteAndOutlet(t,n){let r=n.getContext(t.value.outlet),o=r&&t.value.component?r.children:n,i=rn(t);for(let s of Object.values(i))this.deactivateRouteAndItsChildren(s,o);r&&(r.outlet&&(r.outlet.deactivate(),r.children.onOutletDeactivated()),r.attachRef=null,r.route=null)}activateChildRoutes(t,n,r){let o=rn(n);t.children.forEach(i=>{this.activateRoutes(i,o[i.value.outlet],r),this.forwardEvent(new la(i.value.snapshot))}),t.children.length&&this.forwardEvent(new ca(t.value.snapshot))}activateRoutes(t,n,r){let o=t.value,i=n?n.value:null;if(Ws(o),o===i)if(o.component){let s=r.getOrCreateContext(o.outlet);this.activateChildRoutes(t,n,s.children)}else this.activateChildRoutes(t,n,r);else if(o.component){let s=r.getOrCreateContext(o.outlet);if(this.routeReuseStrategy.shouldAttach(o.snapshot)){let a=this.routeReuseStrategy.retrieve(o.snapshot);this.routeReuseStrategy.store(o.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),Ws(a.route.value),this.activateChildRoutes(t,null,s.children)}else s.attachRef=null,s.route=o,s.outlet&&s.outlet.activateWith(o,s.injector),this.activateChildRoutes(t,null,s.children)}else this.activateChildRoutes(t,null,r)}},Ao=class{constructor(t){this.path=t,this.route=this.path[this.path.length-1]}},an=class{constructor(t,n){this.component=t,this.route=n}};function fR(e,t,n){let r=e._root,o=t?t._root:null;return Vn(r,o,n,[r.value])}function hR(e){let t=e.routeConfig?e.routeConfig.canActivateChild:null;return!t||t.length===0?null:{node:e,guards:t}}function dn(e,t){let n=Symbol(),r=t.get(e,n);return r===n?typeof e=="function"&&!Qc(e)?e:t.get(e):r}function Vn(e,t,n,r,o={canDeactivateChecks:[],canActivateChecks:[]}){let i=rn(t);return e.children.forEach(s=>{pR(s,i[s.value.outlet],n,r.concat([s.value]),o),delete i[s.value.outlet]}),Object.entries(i).forEach(([s,a])=>Hn(a,n.getContext(s),o)),o}function pR(e,t,n,r,o={canDeactivateChecks:[],canActivateChecks:[]}){let i=e.value,s=t?t.value:null,a=n?n.getContext(e.value.outlet):null;if(s&&i.routeConfig===s.routeConfig){let c=ER(s,i,i.routeConfig.runGuardsAndResolvers);c?o.canActivateChecks.push(new Ao(r)):(i.data=s.data,i._resolvedData=s._resolvedData),i.component?Vn(e,t,a?a.children:null,r,o):Vn(e,t,n,r,o),c&&a&&a.outlet&&a.outlet.isActivated&&o.canDeactivateChecks.push(new an(a.outlet.component,s))}else s&&Hn(t,a,o),o.canActivateChecks.push(new Ao(r)),i.component?Vn(e,null,a?a.children:null,r,o):Vn(e,null,n,r,o);return o}function ER(e,t,n){if(typeof n=="function")return n(e,t);switch(n){case"pathParamsChange":return!bt(e.url,t.url);case"pathParamsOrQueryParamsChange":return!bt(e.url,t.url)||!Pe(e.queryParams,t.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!pa(e,t)||!Pe(e.queryParams,t.queryParams);case"paramsChange":default:return!pa(e,t)}}function Hn(e,t,n){let r=rn(e),o=e.value;Object.entries(r).forEach(([i,s])=>{o.component?t?Hn(s,t.children.getContext(i),n):Hn(s,null,n):Hn(s,t,n)}),o.component?t&&t.outlet&&t.outlet.isActivated?n.canDeactivateChecks.push(new an(t.outlet.component,o)):n.canDeactivateChecks.push(new an(null,o)):n.canDeactivateChecks.push(new an(null,o))}function tr(e){return typeof e=="function"}function mR(e){return typeof e=="boolean"}function gR(e){return e&&tr(e.canLoad)}function RR(e){return e&&tr(e.canActivate)}function LR(e){return e&&tr(e.canActivateChild)}function bR(e){return e&&tr(e.canDeactivate)}function vR(e){return e&&tr(e.canMatch)}function nf(e){return e instanceof ke||e?.name==="EmptyError"}var bo=Symbol("INITIAL_VALUE");function ln(){return Se(e=>br(e.map(t=>t.pipe(Fe(1),si(bo)))).pipe(C(t=>{for(let n of t)if(n!==!0){if(n===bo)return bo;if(n===!1||SR(n))return n}return!0}),ve(t=>t!==bo),Fe(1)))}function SR(e){return vt(e)||e instanceof Qn}function yR(e,t){return q(n=>{let{targetSnapshot:r,currentSnapshot:o,guards:{canActivateChecks:i,canDeactivateChecks:s}}=n;return s.length===0&&i.length===0?v($(E({},n),{guardsResult:!0})):DR(s,r,o,e).pipe(q(a=>a&&mR(a)?wR(r,i,e,t):v(a)),C(a=>$(E({},n),{guardsResult:a})))})}function DR(e,t,n,r){return z(e).pipe(q(o=>_R(o.component,o.route,n,t,r)),Ie(o=>o!==!0,!0))}function wR(e,t,n,r){return z(t).pipe(Pt(o=>Ot(IR(o.route.parent,r),CR(o.route,r),MR(e,o.path,n),xR(e,o.route,n))),Ie(o=>o!==!0,!0))}function CR(e,t){return e!==null&&t&&t(new ua(e)),v(!0)}function IR(e,t){return e!==null&&t&&t(new aa(e)),v(!0)}function xR(e,t,n){let r=t.routeConfig?t.routeConfig.canActivate:null;if(!r||r.length===0)return v(!0);let o=r.map(i=>vr(()=>{let s=er(t)??n,a=dn(i,s),c=RR(a)?a.canActivate(t,e):$e(s,()=>a(t,e));return ot(c).pipe(Ie())}));return v(o).pipe(ln())}function MR(e,t,n){let r=t[t.length-1],i=t.slice(0,t.length-1).reverse().map(s=>hR(s)).filter(s=>s!==null).map(s=>vr(()=>{let a=s.guards.map(c=>{let u=er(s.node)??n,l=dn(c,u),d=LR(l)?l.canActivateChild(r,e):$e(u,()=>l(r,e));return ot(d).pipe(Ie())});return v(a).pipe(ln())}));return v(i).pipe(ln())}function _R(e,t,n,r,o){let i=t&&t.routeConfig?t.routeConfig.canDeactivate:null;if(!i||i.length===0)return v(!0);let s=i.map(a=>{let c=er(t)??o,u=dn(a,c),l=bR(u)?u.canDeactivate(e,t,n,r):$e(c,()=>u(e,t,n,r));return ot(l).pipe(Ie())});return v(s).pipe(ln())}function TR(e,t,n,r){let o=t.canLoad;if(o===void 0||o.length===0)return v(!0);let i=o.map(s=>{let a=dn(s,e),c=gR(a)?a.canLoad(t,n):$e(e,()=>a(t,n));return ot(c)});return v(i).pipe(ln(),rf(r))}function rf(e){return Ko(Y(t=>{if(typeof t!="boolean")throw To(e,t)}),C(t=>t===!0))}function AR(e,t,n,r){let o=t.canMatch;if(!o||o.length===0)return v(!0);let i=o.map(s=>{let a=dn(s,e),c=vR(a)?a.canMatch(t,n):$e(e,()=>a(t,n));return ot(c)});return v(i).pipe(ln(),rf(r))}var Kn=class{constructor(t){this.segmentGroup=t||null}},Jn=class extends Error{constructor(t){super(),this.urlTree=t}};function nn(e){return Nt(new Kn(e))}function NR(e){return Nt(new g(4e3,!1))}function OR(e){return Nt(ef(!1,ue.GuardRejected))}var ga=class{constructor(t,n){this.urlSerializer=t,this.urlTree=n}lineralizeSegments(t,n){let r=[],o=n.root;for(;;){if(r=r.concat(o.segments),o.numberOfChildren===0)return v(r);if(o.numberOfChildren>1||!o.children[S])return NR(`${t.redirectTo}`);o=o.children[S]}}applyRedirectCommands(t,n,r,o,i){if(typeof n!="string"){let a=n,{queryParams:c,fragment:u,routeConfig:l,url:d,outlet:p,params:f,data:m,title:w}=o,V=$e(i,()=>a({params:f,data:m,queryParams:c,fragment:u,routeConfig:l,url:d,outlet:p,title:w}));if(V instanceof qe)throw new Jn(V);n=V}let s=this.applyRedirectCreateUrlTree(n,this.urlSerializer.parse(n),t,r);if(n[0]==="/")throw new Jn(s);return s}applyRedirectCreateUrlTree(t,n,r,o){let i=this.createSegmentGroup(t,n.root,r,o);return new qe(i,this.createQueryParams(n.queryParams,this.urlTree.queryParams),n.fragment)}createQueryParams(t,n){let r={};return Object.entries(t).forEach(([o,i])=>{if(typeof i=="string"&&i[0]===":"){let a=i.substring(1);r[o]=n[a]}else r[o]=i}),r}createSegmentGroup(t,n,r,o){let i=this.createSegments(t,n.segments,r,o),s={};return Object.entries(n.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(t,c,r,o)}),new O(i,s)}createSegments(t,n,r,o){return n.map(i=>i.path[0]===":"?this.findPosParam(t,i,o):this.findOrReturn(i,r))}findPosParam(t,n,r){let o=r[n.path.substring(1)];if(!o)throw new g(4001,!1);return o}findOrReturn(t,n){let r=0;for(let o of n){if(o.path===t.path)return n.splice(r),o;r++}return t}},Ra={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function PR(e,t,n,r,o){let i=of(e,t,n);return i.matched?(r=oR(t,r),AR(r,t,n,o).pipe(C(s=>s===!0?i:E({},Ra)))):v(i)}function of(e,t,n){if(t.path==="**")return kR(n);if(t.path==="")return t.pathMatch==="full"&&(e.hasChildren()||n.length>0)?E({},Ra):{matched:!0,consumedSegments:[],remainingSegments:n,parameters:{},positionalParamSegments:{}};let o=(t.matcher||Ag)(n,e,t);if(!o)return E({},Ra);let i={};Object.entries(o.posParams??{}).forEach(([a,c])=>{i[a]=c.path});let s=o.consumed.length>0?E(E({},i),o.consumed[o.consumed.length-1].parameters):i;return{matched:!0,consumedSegments:o.consumed,remainingSegments:n.slice(o.consumed.length),parameters:s,positionalParamSegments:o.posParams??{}}}function kR(e){return{matched:!0,parameters:e.length>0?Fd(e).parameters:{},consumedSegments:e,remainingSegments:[],positionalParamSegments:{}}}function Nd(e,t,n,r){return n.length>0&&VR(e,n,r)?{segmentGroup:new O(t,jR(r,new O(n,e.children))),slicedSegments:[]}:n.length===0&&BR(e,n,r)?{segmentGroup:new O(e.segments,FR(e,n,r,e.children)),slicedSegments:n}:{segmentGroup:new O(e.segments,e.children),slicedSegments:n}}function FR(e,t,n,r){let o={};for(let i of n)if(Oo(e,t,i)&&!r[Ce(i)]){let s=new O([],{});o[Ce(i)]=s}return E(E({},r),o)}function jR(e,t){let n={};n[S]=t;for(let r of e)if(r.path===""&&Ce(r)!==S){let o=new O([],{});n[Ce(r)]=o}return n}function VR(e,t,n){return n.some(r=>Oo(e,t,r)&&Ce(r)!==S)}function BR(e,t,n){return n.some(r=>Oo(e,t,r))}function Oo(e,t,n){return(e.hasChildren()||t.length>0)&&n.pathMatch==="full"?!1:n.path===""}function $R(e,t,n){return t.length===0&&!e.children[n]}var La=class{};function UR(e,t,n,r,o,i,s="emptyOnly"){return new ba(e,t,n,r,o,s,i).recognize()}var HR=31,ba=class{constructor(t,n,r,o,i,s,a){this.injector=t,this.configLoader=n,this.rootComponentType=r,this.config=o,this.urlTree=i,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.applyRedirects=new ga(this.urlSerializer,this.urlTree),this.absoluteRedirectCount=0,this.allowRedirects=!0}noMatchError(t){return new g(4002,`'${t.segmentGroup}'`)}recognize(){let t=Nd(this.urlTree.root,[],[],this.config).segmentGroup;return this.match(t).pipe(C(({children:n,rootSnapshot:r})=>{let o=new ce(r,n),i=new _o("",o),s=Kg(r,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,i.url=this.urlSerializer.serialize(s),{state:i,tree:s}}))}match(t){let n=new sn([],Object.freeze({}),Object.freeze(E({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),S,this.rootComponentType,null,{});return this.processSegmentGroup(this.injector,this.config,t,S,n).pipe(C(r=>({children:r,rootSnapshot:n})),We(r=>{if(r instanceof Jn)return this.urlTree=r.urlTree,this.match(r.urlTree.root);throw r instanceof Kn?this.noMatchError(r):r}))}processSegmentGroup(t,n,r,o,i){return r.segments.length===0&&r.hasChildren()?this.processChildren(t,n,r,i):this.processSegment(t,n,r,r.segments,o,!0,i).pipe(C(s=>s instanceof ce?[s]:[]))}processChildren(t,n,r,o){let i=[];for(let s of Object.keys(r.children))s==="primary"?i.unshift(s):i.push(s);return z(i).pipe(Pt(s=>{let a=r.children[s],c=iR(n,s);return this.processSegmentGroup(t,c,a,s,o)}),ii((s,a)=>(s.push(...a),s)),Ze(null),oi(),q(s=>{if(s===null)return nn(r);let a=sf(s);return zR(a),v(a)}))}processSegment(t,n,r,o,i,s,a){return z(n).pipe(Pt(c=>this.processSegmentAgainstRoute(c._injector??t,n,c,r,o,i,s,a).pipe(We(u=>{if(u instanceof Kn)return v(null);throw u}))),Ie(c=>!!c),We(c=>{if(nf(c))return $R(r,o,i)?v(new La):nn(r);throw c}))}processSegmentAgainstRoute(t,n,r,o,i,s,a,c){return Ce(r)!==s&&(s===S||!Oo(o,i,r))?nn(o):r.redirectTo===void 0?this.matchSegmentAgainstRoute(t,o,r,i,s,c):this.allowRedirects&&a?this.expandSegmentAgainstRouteUsingRedirect(t,o,n,r,i,s,c):nn(o)}expandSegmentAgainstRouteUsingRedirect(t,n,r,o,i,s,a){let{matched:c,parameters:u,consumedSegments:l,positionalParamSegments:d,remainingSegments:p}=of(n,o,i);if(!c)return nn(n);typeof o.redirectTo=="string"&&o.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>HR&&(this.allowRedirects=!1));let f=new sn(i,u,Object.freeze(E({},this.urlTree.queryParams)),this.urlTree.fragment,Od(o),Ce(o),o.component??o._loadedComponent??null,o,Pd(o)),m=Mo(f,a,this.paramsInheritanceStrategy);f.params=Object.freeze(m.params),f.data=Object.freeze(m.data);let w=this.applyRedirects.applyRedirectCommands(l,o.redirectTo,d,f,t);return this.applyRedirects.lineralizeSegments(o,w).pipe(q(V=>this.processSegment(t,r,n,V.concat(p),s,!1,a)))}matchSegmentAgainstRoute(t,n,r,o,i,s){let a=PR(n,r,o,t,this.urlSerializer);return r.path==="**"&&(n.children={}),a.pipe(Se(c=>c.matched?(t=r._injector??t,this.getChildConfig(t,r,o).pipe(Se(({routes:u})=>{let l=r._loadedInjector??t,{parameters:d,consumedSegments:p,remainingSegments:f}=c,m=new sn(p,d,Object.freeze(E({},this.urlTree.queryParams)),this.urlTree.fragment,Od(r),Ce(r),r.component??r._loadedComponent??null,r,Pd(r)),w=Mo(m,s,this.paramsInheritanceStrategy);m.params=Object.freeze(w.params),m.data=Object.freeze(w.data);let{segmentGroup:V,slicedSegments:B}=Nd(n,p,f,u);if(B.length===0&&V.hasChildren())return this.processChildren(l,u,V,m).pipe(C(te=>new ce(m,te)));if(u.length===0&&B.length===0)return v(new ce(m,[]));let ee=Ce(r)===i;return this.processSegment(l,u,V,B,ee?S:i,!0,m).pipe(C(te=>new ce(m,te instanceof ce?[te]:[])))}))):nn(n)))}getChildConfig(t,n,r){return n.children?v({routes:n.children,injector:t}):n.loadChildren?n._loadedRoutes!==void 0?v({routes:n._loadedRoutes,injector:n._loadedInjector}):TR(t,n,r,this.urlSerializer).pipe(q(o=>o?this.configLoader.loadChildren(t,n).pipe(Y(i=>{n._loadedRoutes=i.routes,n._loadedInjector=i.injector})):OR(n))):v({routes:[],injector:t})}};function zR(e){e.sort((t,n)=>t.value.outlet===S?-1:n.value.outlet===S?1:t.value.outlet.localeCompare(n.value.outlet))}function qR(e){let t=e.value.routeConfig;return t&&t.path===""}function sf(e){let t=[],n=new Set;for(let r of e){if(!qR(r)){t.push(r);continue}let o=t.find(i=>r.value.routeConfig===i.value.routeConfig);o!==void 0?(o.children.push(...r.children),n.add(o)):t.push(r)}for(let r of n){let o=sf(r.children);t.push(new ce(r.value,o))}return t.filter(r=>!n.has(r))}function Od(e){return e.data||{}}function Pd(e){return e.resolve||{}}function GR(e,t,n,r,o,i){return q(s=>UR(e,t,n,r,s.extractedUrl,o,i).pipe(C(({state:a,tree:c})=>$(E({},s),{targetSnapshot:a,urlAfterRedirects:c}))))}function WR(e,t){return q(n=>{let{targetSnapshot:r,guards:{canActivateChecks:o}}=n;if(!o.length)return v(n);let i=new Set(o.map(c=>c.route)),s=new Set;for(let c of i)if(!s.has(c))for(let u of af(c))s.add(u);let a=0;return z(s).pipe(Pt(c=>i.has(c)?ZR(c,r,e,t):(c.data=Mo(c,c.parent,e).resolve,v(void 0))),Y(()=>a++),kt(1),q(c=>a===s.size?v(n):se))})}function af(e){let t=e.children.map(n=>af(n)).flat();return[e,...t]}function ZR(e,t,n,r){let o=e.routeConfig,i=e._resolve;return o?.title!==void 0&&!Jd(o)&&(i[Xn]=o.title),YR(i,e,t,r).pipe(C(s=>(e._resolvedData=s,e.data=Mo(e,e.parent,n).resolve,null)))}function YR(e,t,n,r){let o=Qs(e);if(o.length===0)return v({});let i={};return z(o).pipe(q(s=>QR(e[s],t,n,r).pipe(Ie(),Y(a=>{if(a instanceof Qn)throw To(new zn,a);i[s]=a}))),kt(1),ri(i),We(s=>nf(s)?se:Nt(s)))}function QR(e,t,n,r){let o=er(t)??r,i=dn(e,o),s=i.resolve?i.resolve(t,n):$e(o,()=>i(t,n));return ot(s)}function Zs(e){return Se(t=>{let n=e(t);return n?z(n).pipe(C(()=>t)):v(t)})}var cf=(()=>{class e{buildTitle(n){let r,o=n.root;for(;o!==void 0;)r=this.getResolvedTitleForRoute(o)??r,o=o.children.find(i=>i.outlet===S);return r}getResolvedTitleForRoute(n){return n.data[Xn]}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:()=>h(KR),providedIn:"root"})}}return e})(),KR=(()=>{class e extends cf{constructor(n){super(),this.title=n}updateTitle(n){let r=this.buildTitle(n);r!==void 0&&this.title.setTitle(r)}static{this.\u0275fac=function(r){return new(r||e)(x(xd))}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})(),Ca=new y("",{providedIn:"root",factory:()=>({})}),JR=(()=>{class e{static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275cmp=et({type:e,selectors:[["ng-component"]],standalone:!0,features:[nt],decls:1,vars:0,template:function(r,o){r&1&&Oe(0,"router-outlet")},dependencies:[Da],encapsulation:2})}}return e})();function Ia(e){let t=e.children&&e.children.map(Ia),n=t?$(E({},e),{children:t}):E({},e);return!n.component&&!n.loadComponent&&(t||n.loadChildren)&&n.outlet&&n.outlet!==S&&(n.component=JR),n}var xa=new y(""),XR=(()=>{class e{constructor(){this.componentLoaders=new WeakMap,this.childrenLoaders=new WeakMap,this.compiler=h(Ps)}loadComponent(n){if(this.componentLoaders.get(n))return this.componentLoaders.get(n);if(n._loadedComponent)return v(n._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(n);let r=ot(n.loadComponent()).pipe(C(uf),Y(i=>{this.onLoadEndListener&&this.onLoadEndListener(n),n._loadedComponent=i}),pn(()=>{this.componentLoaders.delete(n)})),o=new At(r,()=>new K).pipe(Tt());return this.componentLoaders.set(n,o),o}loadChildren(n,r){if(this.childrenLoaders.get(r))return this.childrenLoaders.get(r);if(r._loadedRoutes)return v({routes:r._loadedRoutes,injector:r._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(r);let i=e0(r,this.compiler,n,this.onLoadEndListener).pipe(pn(()=>{this.childrenLoaders.delete(r)})),s=new At(i,()=>new K).pipe(Tt());return this.childrenLoaders.set(r,s),s}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})();function e0(e,t,n,r){return ot(e.loadChildren()).pipe(C(uf),q(o=>o instanceof Cn||Array.isArray(o)?v(o):z(t.compileModuleAsync(o))),C(o=>{r&&r(e);let i,s,a=!1;return Array.isArray(o)?(s=o,a=!0):(i=o.create(n).injector,s=i.get(xa,[],{optional:!0,self:!0}).flat()),{routes:s.map(Ia),injector:i}}))}function t0(e){return e&&typeof e=="object"&&"default"in e}function uf(e){return t0(e)?e.default:e}var Ma=(()=>{class e{static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:()=>h(n0),providedIn:"root"})}}return e})(),n0=(()=>{class e{shouldProcessUrl(n){return!0}extract(n){return n}merge(n,r){return n}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})(),r0=new y("");var o0=new y(""),i0=(()=>{class e{get hasRequestedNavigation(){return this.navigationId!==0}constructor(){this.currentNavigation=null,this.currentTransition=null,this.lastSuccessfulNavigation=null,this.events=new K,this.transitionAbortSubject=new K,this.configLoader=h(XR),this.environmentInjector=h(pe),this.urlSerializer=h(Sa),this.rootContexts=h(No),this.location=h(On),this.inputBindingEnabled=h(wa,{optional:!0})!==null,this.titleStrategy=h(cf),this.options=h(Ca,{optional:!0})||{},this.paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly",this.urlHandlingStrategy=h(Ma),this.createViewTransition=h(r0,{optional:!0}),this.navigationErrorHandler=h(o0,{optional:!0}),this.navigationId=0,this.afterPreactivation=()=>v(void 0),this.rootComponentType=null;let n=o=>this.events.next(new ia(o)),r=o=>this.events.next(new sa(o));this.configLoader.onLoadEndListener=r,this.configLoader.onLoadStartListener=n}complete(){this.transitions?.complete()}handleNavigationRequest(n){let r=++this.navigationId;this.transitions?.next($(E(E({},this.transitions.value),n),{id:r}))}setupNavigations(n,r,o){return this.transitions=new Z({id:0,currentUrlTree:r,currentRawUrl:r,extractedUrl:this.urlHandlingStrategy.extract(r),urlAfterRedirects:this.urlHandlingStrategy.extract(r),rawUrl:r,extras:{},resolve:()=>{},reject:()=>{},promise:Promise.resolve(!0),source:Un,restoredState:null,currentSnapshot:o.snapshot,targetSnapshot:null,currentRouterState:o,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null}),this.transitions.pipe(ve(i=>i.id!==0),C(i=>$(E({},i),{extractedUrl:this.urlHandlingStrategy.extract(i.rawUrl)})),Se(i=>{let s=!1,a=!1;return v(i).pipe(Se(c=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",ue.SupersededByNewNavigation),se;this.currentTransition=i,this.currentNavigation={id:c.id,initialUrl:c.rawUrl,extractedUrl:c.extractedUrl,targetBrowserUrl:typeof c.extras.browserUrl=="string"?this.urlSerializer.parse(c.extras.browserUrl):c.extras.browserUrl,trigger:c.source,extras:c.extras,previousNavigation:this.lastSuccessfulNavigation?$(E({},this.lastSuccessfulNavigation),{previousNavigation:null}):null};let u=!n.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),l=c.extras.onSameUrlNavigation??n.onSameUrlNavigation;if(!u&&l!=="reload"){let d="";return this.events.next(new St(c.id,this.urlSerializer.serialize(c.rawUrl),d,ea.IgnoredSameUrlNavigation)),c.resolve(!1),se}if(this.urlHandlingStrategy.shouldProcessUrl(c.rawUrl))return v(c).pipe(Se(d=>{let p=this.transitions?.getValue();return this.events.next(new Gn(d.id,this.urlSerializer.serialize(d.extractedUrl),d.source,d.restoredState)),p!==this.transitions?.getValue()?se:Promise.resolve(d)}),GR(this.environmentInjector,this.configLoader,this.rootComponentType,n.config,this.urlSerializer,this.paramsInheritanceStrategy),Y(d=>{i.targetSnapshot=d.targetSnapshot,i.urlAfterRedirects=d.urlAfterRedirects,this.currentNavigation=$(E({},this.currentNavigation),{finalUrl:d.urlAfterRedirects});let p=new Co(d.id,this.urlSerializer.serialize(d.extractedUrl),this.urlSerializer.serialize(d.urlAfterRedirects),d.targetSnapshot);this.events.next(p)}));if(u&&this.urlHandlingStrategy.shouldProcessUrl(c.currentRawUrl)){let{id:d,extractedUrl:p,source:f,restoredState:m,extras:w}=c,V=new Gn(d,this.urlSerializer.serialize(p),f,m);this.events.next(V);let B=Qd(this.rootComponentType).snapshot;return this.currentTransition=i=$(E({},c),{targetSnapshot:B,urlAfterRedirects:p,extras:$(E({},w),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.finalUrl=p,v(i)}else{let d="";return this.events.next(new St(c.id,this.urlSerializer.serialize(c.extractedUrl),d,ea.IgnoredByUrlHandlingStrategy)),c.resolve(!1),se}}),Y(c=>{let u=new ta(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(u)}),C(c=>(this.currentTransition=i=$(E({},c),{guards:fR(c.targetSnapshot,c.currentSnapshot,this.rootContexts)}),i)),yR(this.environmentInjector,c=>this.events.next(c)),Y(c=>{if(i.guardsResult=c.guardsResult,c.guardsResult&&typeof c.guardsResult!="boolean")throw To(this.urlSerializer,c.guardsResult);let u=new na(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot,!!c.guardsResult);this.events.next(u)}),ve(c=>c.guardsResult?!0:(this.cancelNavigationTransition(c,"",ue.GuardRejected),!1)),Zs(c=>{if(c.guards.canActivateChecks.length)return v(c).pipe(Y(u=>{let l=new ra(u.id,this.urlSerializer.serialize(u.extractedUrl),this.urlSerializer.serialize(u.urlAfterRedirects),u.targetSnapshot);this.events.next(l)}),Se(u=>{let l=!1;return v(u).pipe(WR(this.paramsInheritanceStrategy,this.environmentInjector),Y({next:()=>l=!0,complete:()=>{l||this.cancelNavigationTransition(u,"",ue.NoDataFromResolver)}}))}),Y(u=>{let l=new oa(u.id,this.urlSerializer.serialize(u.extractedUrl),this.urlSerializer.serialize(u.urlAfterRedirects),u.targetSnapshot);this.events.next(l)}))}),Zs(c=>{let u=l=>{let d=[];l.routeConfig?.loadComponent&&!l.routeConfig._loadedComponent&&d.push(this.configLoader.loadComponent(l.routeConfig).pipe(Y(p=>{l.component=p}),C(()=>{})));for(let p of l.children)d.push(...u(p));return d};return br(u(c.targetSnapshot.root)).pipe(Ze(null),Fe(1))}),Zs(()=>this.afterPreactivation()),Se(()=>{let{currentSnapshot:c,targetSnapshot:u}=i,l=this.createViewTransition?.(this.environmentInjector,c.root,u.root);return l?z(l).pipe(C(()=>i)):v(i)}),C(c=>{let u=aR(n.routeReuseStrategy,c.targetSnapshot,c.currentRouterState);return this.currentTransition=i=$(E({},c),{targetRouterState:u}),this.currentNavigation.targetRouterState=u,i}),Y(()=>{this.events.next(new Zn)}),dR(this.rootContexts,n.routeReuseStrategy,c=>this.events.next(c),this.inputBindingEnabled),Fe(1),Y({next:c=>{s=!0,this.lastSuccessfulNavigation=this.currentNavigation,this.events.next(new rt(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects))),this.titleStrategy?.updateTitle(c.targetRouterState.snapshot),c.resolve(!0)},complete:()=>{s=!0}}),ai(this.transitionAbortSubject.pipe(Y(c=>{throw c}))),pn(()=>{!s&&!a&&this.cancelNavigationTransition(i,"",ue.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation=null,this.currentTransition=null)}),We(c=>{if(a=!0,tf(c))this.events.next(new ze(i.id,this.urlSerializer.serialize(i.extractedUrl),c.message,c.cancellationCode)),lR(c)?this.events.next(new un(c.url,c.navigationBehaviorOptions)):i.resolve(!1);else{let u=new Wn(i.id,this.urlSerializer.serialize(i.extractedUrl),c,i.targetSnapshot??void 0);try{let l=$e(this.environmentInjector,()=>this.navigationErrorHandler?.(u));if(l instanceof Qn){let{message:d,cancellationCode:p}=To(this.urlSerializer,l);this.events.next(new ze(i.id,this.urlSerializer.serialize(i.extractedUrl),d,p)),this.events.next(new un(l.redirectTo,l.navigationBehaviorOptions))}else{this.events.next(u);let d=n.errorHandler(c);i.resolve(!!d)}}catch(l){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(l)}}return se}))}))}cancelNavigationTransition(n,r,o){let i=new ze(n.id,this.urlSerializer.serialize(n.extractedUrl),r,o);this.events.next(i),n.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let n=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),r=this.currentNavigation?.targetBrowserUrl??this.currentNavigation?.extractedUrl;return n.toString()!==r?.toString()&&!this.currentNavigation?.extras.skipLocationChange}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})();function s0(e){return e!==Un}var a0=(()=>{class e{static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:()=>h(c0),providedIn:"root"})}}return e})(),va=class{shouldDetach(t){return!1}store(t,n){}shouldAttach(t){return!1}retrieve(t){return null}shouldReuseRoute(t,n){return t.routeConfig===n.routeConfig}},c0=(()=>{class e extends va{static{this.\u0275fac=(()=>{let n;return function(o){return(n||(n=us(e)))(o||e)}})()}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})(),lf=(()=>{class e{static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:()=>h(u0),providedIn:"root"})}}return e})(),u0=(()=>{class e extends lf{constructor(){super(...arguments),this.location=h(On),this.urlSerializer=h(Sa),this.options=h(Ca,{optional:!0})||{},this.canceledNavigationResolution=this.options.canceledNavigationResolution||"replace",this.urlHandlingStrategy=h(Ma),this.urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred",this.currentUrlTree=new qe,this.rawUrlTree=this.currentUrlTree,this.currentPageId=0,this.lastSuccessfulId=-1,this.routerState=Qd(null),this.stateMemento=this.createStateMemento()}getCurrentUrlTree(){return this.currentUrlTree}getRawUrlTree(){return this.rawUrlTree}restoredState(){return this.location.getState()}get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}getRouterState(){return this.routerState}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}registerNonRouterCurrentEntryChangeListener(n){return this.location.subscribe(r=>{r.type==="popstate"&&n(r.url,r.state)})}handleRouterEvent(n,r){if(n instanceof Gn)this.stateMemento=this.createStateMemento();else if(n instanceof St)this.rawUrlTree=r.initialUrl;else if(n instanceof Co){if(this.urlUpdateStrategy==="eager"&&!r.extras.skipLocationChange){let o=this.urlHandlingStrategy.merge(r.finalUrl,r.initialUrl);this.setBrowserUrl(r.targetBrowserUrl??o,r)}}else n instanceof Zn?(this.currentUrlTree=r.finalUrl,this.rawUrlTree=this.urlHandlingStrategy.merge(r.finalUrl,r.initialUrl),this.routerState=r.targetRouterState,this.urlUpdateStrategy==="deferred"&&!r.extras.skipLocationChange&&this.setBrowserUrl(r.targetBrowserUrl??this.rawUrlTree,r)):n instanceof ze&&(n.code===ue.GuardRejected||n.code===ue.NoDataFromResolver)?this.restoreHistory(r):n instanceof Wn?this.restoreHistory(r,!0):n instanceof rt&&(this.lastSuccessfulId=n.id,this.currentPageId=this.browserPageId)}setBrowserUrl(n,r){let o=n instanceof qe?this.urlSerializer.serialize(n):n;if(this.location.isCurrentPathEqualTo(o)||r.extras.replaceUrl){let i=this.browserPageId,s=E(E({},r.extras.state),this.generateNgRouterState(r.id,i));this.location.replaceState(o,"",s)}else{let i=E(E({},r.extras.state),this.generateNgRouterState(r.id,this.browserPageId+1));this.location.go(o,"",i)}}restoreHistory(n,r=!1){if(this.canceledNavigationResolution==="computed"){let o=this.browserPageId,i=this.currentPageId-o;i!==0?this.location.historyGo(i):this.currentUrlTree===n.finalUrl&&i===0&&(this.resetState(n),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(r&&this.resetState(n),this.resetUrlToCurrentUrlTree())}resetState(n){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,n.finalUrl??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.rawUrlTree),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(n,r){return this.canceledNavigationResolution==="computed"?{navigationId:n,\u0275routerPageId:r}:{navigationId:n}}static{this.\u0275fac=(()=>{let n;return function(o){return(n||(n=us(e)))(o||e)}})()}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})(),Bn=function(e){return e[e.COMPLETE=0]="COMPLETE",e[e.FAILED=1]="FAILED",e[e.REDIRECTING=2]="REDIRECTING",e}(Bn||{});function l0(e,t){e.events.pipe(ve(n=>n instanceof rt||n instanceof ze||n instanceof Wn||n instanceof St),C(n=>n instanceof rt||n instanceof St?Bn.COMPLETE:(n instanceof ze?n.code===ue.Redirect||n.code===ue.SupersededByNewNavigation:!1)?Bn.REDIRECTING:Bn.FAILED),ve(n=>n!==Bn.REDIRECTING),Fe(1)).subscribe(()=>{t()})}function d0(e){throw e}var f0={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},h0={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"},nr=(()=>{class e{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}constructor(){this.disposed=!1,this.console=h(po),this.stateManager=h(lf),this.options=h(Ca,{optional:!0})||{},this.pendingTasks=h(Kt),this.urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred",this.navigationTransitions=h(i0),this.urlSerializer=h(Sa),this.location=h(On),this.urlHandlingStrategy=h(Ma),this._events=new K,this.errorHandler=this.options.errorHandler||d0,this.navigated=!1,this.routeReuseStrategy=h(a0),this.onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore",this.config=h(xa,{optional:!0})?.flat()??[],this.componentInputBindingEnabled=!!h(wa,{optional:!0}),this.eventsSubscription=new U,this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this,this.currentUrlTree,this.routerState).subscribe({error:n=>{this.console.warn(n)}}),this.subscribeToNavigationEvents()}subscribeToNavigationEvents(){let n=this.navigationTransitions.events.subscribe(r=>{try{let o=this.navigationTransitions.currentTransition,i=this.navigationTransitions.currentNavigation;if(o!==null&&i!==null){if(this.stateManager.handleRouterEvent(r,i),r instanceof ze&&r.code!==ue.Redirect&&r.code!==ue.SupersededByNewNavigation)this.navigated=!0;else if(r instanceof rt)this.navigated=!0;else if(r instanceof un){let s=r.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(r.url,o.currentRawUrl),c=E({browserUrl:o.extras.browserUrl,info:o.extras.info,skipLocationChange:o.extras.skipLocationChange,replaceUrl:o.extras.replaceUrl||this.urlUpdateStrategy==="eager"||s0(o.source)},s);this.scheduleNavigation(a,Un,null,c,{resolve:o.resolve,reject:o.reject,promise:o.promise})}}E0(r)&&this._events.next(r)}catch(o){this.navigationTransitions.transitionAbortSubject.next(o)}});this.eventsSubscription.add(n)}resetRootComponentType(n){this.routerState.root.component=n,this.navigationTransitions.rootComponentType=n}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Un,this.stateManager.restoredState())}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((n,r)=>{setTimeout(()=>{this.navigateToSyncWithBrowser(n,"popstate",r)},0)})}navigateToSyncWithBrowser(n,r,o){let i={replaceUrl:!0},s=o?.navigationId?o:null;if(o){let c=E({},o);delete c.navigationId,delete c.\u0275routerPageId,Object.keys(c).length!==0&&(i.state=c)}let a=this.parseUrl(n);this.scheduleNavigation(a,r,s,i)}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return this.navigationTransitions.currentNavigation}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(n){this.config=n.map(Ia),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription&&(this.nonRouterCurrentEntryChangeSubscription.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0),this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(n,r={}){let{relativeTo:o,queryParams:i,fragment:s,queryParamsHandling:a,preserveFragment:c}=r,u=c?this.currentUrlTree.fragment:s,l=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":l=E(E({},this.currentUrlTree.queryParams),i);break;case"preserve":l=this.currentUrlTree.queryParams;break;default:l=i||null}l!==null&&(l=this.removeEmptyProps(l));let d;try{let p=o?o.snapshot:this.routerState.snapshot.root;d=Gd(p)}catch{(typeof n[0]!="string"||n[0][0]!=="/")&&(n=[]),d=this.currentUrlTree.root}return Wd(d,n,l,u??null)}navigateByUrl(n,r={skipLocationChange:!1}){let o=vt(n)?n:this.parseUrl(n),i=this.urlHandlingStrategy.merge(o,this.rawUrlTree);return this.scheduleNavigation(i,Un,null,r)}navigate(n,r={skipLocationChange:!1}){return p0(n),this.navigateByUrl(this.createUrlTree(n,r),r)}serializeUrl(n){return this.urlSerializer.serialize(n)}parseUrl(n){try{return this.urlSerializer.parse(n)}catch{return this.urlSerializer.parse("/")}}isActive(n,r){let o;if(r===!0?o=E({},f0):r===!1?o=E({},h0):o=r,vt(n))return Md(this.currentUrlTree,n,o);let i=this.parseUrl(n);return Md(this.currentUrlTree,i,o)}removeEmptyProps(n){return Object.entries(n).reduce((r,[o,i])=>(i!=null&&(r[o]=i),r),{})}scheduleNavigation(n,r,o,i,s){if(this.disposed)return Promise.resolve(!1);let a,c,u;s?(a=s.resolve,c=s.reject,u=s.promise):u=new Promise((d,p)=>{a=d,c=p});let l=this.pendingTasks.add();return l0(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(l))}),this.navigationTransitions.handleNavigationRequest({source:r,restoredState:o,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:n,extras:i,resolve:a,reject:c,promise:u,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),u.catch(d=>Promise.reject(d))}static{this.\u0275fac=function(r){return new(r||e)}}static{this.\u0275prov=b({token:e,factory:e.\u0275fac,providedIn:"root"})}}return e})();function p0(e){for(let t=0;t<e.length;t++)if(e[t]==null)throw new g(4008,!1)}function E0(e){return!(e instanceof Zn)&&!(e instanceof un)}var df=(()=>{class e{constructor(n,r,o,i,s,a){this.router=n,this.route=r,this.tabIndexAttribute=o,this.renderer=i,this.el=s,this.locationStrategy=a,this.href=null,this.onChanges=new K,this.preserveFragment=!1,this.skipLocationChange=!1,this.replaceUrl=!1,this.routerLinkInput=null;let c=s.nativeElement.tagName?.toLowerCase();this.isAnchorElement=c==="a"||c==="area",this.isAnchorElement?this.subscription=n.events.subscribe(u=>{u instanceof rt&&this.updateHref()}):this.setTabIndexIfNotOnNativeEl("0")}setTabIndexIfNotOnNativeEl(n){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",n)}ngOnChanges(n){this.isAnchorElement&&this.updateHref(),this.onChanges.next(this)}set routerLink(n){n==null?(this.routerLinkInput=null,this.setTabIndexIfNotOnNativeEl(null)):(vt(n)?this.routerLinkInput=n:this.routerLinkInput=Array.isArray(n)?n:[n],this.setTabIndexIfNotOnNativeEl("0"))}onClick(n,r,o,i,s){let a=this.urlTree;if(a===null||this.isAnchorElement&&(n!==0||r||o||i||s||typeof this.target=="string"&&this.target!="_self"))return!0;let c={skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info};return this.router.navigateByUrl(a,c),!this.isAnchorElement}ngOnDestroy(){this.subscription?.unsubscribe()}updateHref(){let n=this.urlTree;this.href=n!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(n)):null;let r=this.href===null?null:gl(this.href,this.el.nativeElement.tagName.toLowerCase(),"href");this.applyAttributeValue("href",r)}applyAttributeValue(n,r){let o=this.renderer,i=this.el.nativeElement;r!==null?o.setAttribute(i,n,r):o.removeAttribute(i,n)}get urlTree(){return this.routerLinkInput===null?null:vt(this.routerLinkInput)?this.routerLinkInput:this.router.createUrlTree(this.routerLinkInput,{relativeTo:this.relativeTo!==void 0?this.relativeTo:this.route,queryParams:this.queryParams,fragment:this.fragment,queryParamsHandling:this.queryParamsHandling,preserveFragment:this.preserveFragment})}static{this.\u0275fac=function(r){return new(r||e)(Ne(nr),Ne(yt),ls("tabindex"),Ne(fo),Ne(_n),Ne(tn))}}static{this.\u0275dir=Jr({type:e,selectors:[["","routerLink",""]],hostVars:1,hostBindings:function(r,o){r&1&&Ns("click",function(s){return o.onClick(s.button,s.ctrlKey,s.shiftKey,s.altKey,s.metaKey)}),r&2&&Ts("target",o.target)},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",Nn],skipLocationChange:[2,"skipLocationChange","skipLocationChange",Nn],replaceUrl:[2,"replaceUrl","replaceUrl",Nn],routerLink:"routerLink"},standalone:!0,features:[Ms,Mn]})}}return e})();var m0=new y("");function ff(e,...t){return Xr([{provide:xa,multi:!0,useValue:e},[],{provide:yt,useFactory:g0,deps:[nr]},{provide:Os,multi:!0,useFactory:R0},t.map(n=>n.\u0275providers)])}function g0(e){return e.routerState.root}function R0(){let e=h(pt);return t=>{let n=e.get(gt);if(t!==n.components[0])return;let r=e.get(nr),o=e.get(L0);e.get(b0)===1&&r.initialNavigation(),e.get(v0,null,D.Optional)?.setUpPreloading(),e.get(m0,null,D.Optional)?.init(),r.resetRootComponentType(n.componentTypes[0]),o.closed||(o.next(),o.complete(),o.unsubscribe())}}var L0=new y("",{factory:()=>new K}),b0=new y("",{providedIn:"root",factory:()=>1});var v0=new y("");var Po=(()=>{class e{router;title="home20250523";constructor(n){this.router=n}getTXT(){console.log("get txt "),this.router.navigate(["/home/app-ads.txt"])}static \u0275fac=function(r){return new(r||e)(Ne(nr))};static \u0275cmp=et({type:e,selectors:[["app-root"]],standalone:!0,features:[nt],decls:1,vars:0,template:function(r,o){r&1&&Oe(0,"router-outlet")},dependencies:[Da]})}return e})();var hf=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=et({type:e,selectors:[["app-txt"]],standalone:!0,features:[nt],decls:2,vars:0,consts:[[2,"word-wrap","break-word","white-space","pre-wrap"]],template:function(r,o){r&1&&(T(0,"pre",0),F(1,`google.com, pub-4054029324549436, DIRECT, f08c47fec0942fa0

    fyber.com,219661,DIRECT
    acd.op.hicloud.com,PUB_HW_1003,RESELLER
    acexchange.co.kr,1854269911,RESELLER
    adelement.com,48222,RESELLER
    adform.com,1762,RESELLER
    adform.com,2688,RESELLER,9f5210a2f0999e32
    adform.com,2795,RESELLER,9f5210a2f0999e32
    adform.com,2845,RESELLER,9f5210a2f0999e32
    adform.com,2967,RESELLER,9f5210a2f0999e32
    adform.com,3016,RESELLER,9f5210a2f0999e32
    adiiix.com,db716a4c,RESELLER
    adingo.jp,31054,RESELLER
    admixer.net,26f19e7a-1b46-4e44-a635-e92c6a27b88b,RESELLER
    advertising.com,28650,RESELLER
    adx-dra.op.hicloud.com,PUB_HW_1003,RESELLER
    adx-dre.op.hicloud.com,PUB_HW_1003,RESELLER
    adx-drru.op.hicloud.com,PUB_HW_1003,RESELLER
    adyoulike.com,a2226c27fc2a6773f6a2b365e013513a,RESELLER
    algorix.co,54236,RESELLER,5b394c12fea27a1d
    app-stock.com,558223,RESELLER,ed8c126ea5971415
    app-stock.com,91654,RESELLER,ed8c126ea5971415
    appnexus.com,13258,RESELLER,f5ab79cb980f11d1
    appnexus.com,13293,RESELLER,f5ab79cb980f11d1
    appnexus.com,13774,RESELLER,f5ab79cb980f11d1
    appnexus.com,13956,RESELLER,f5ab79cb980f11d1
    appnexus.com,14538,RESELLER,f5ab79cb980f11d1
    appnexus.com,15349,RESELLER,f5ab79cb980f11d1
    appnexus.com,15426,RESELLER,f5ab79cb980f11d1
    appnexus.com,15670,RESELLER,f5ab79cb980f11d1
    appnexus.com,15980,RESELLER,f5ab79cb980f11d1
    appsheep.com,225239742b9b4299988a6ac86d11f752,RESELLER
    aralego.com,par-E2B3A33EAA4286BEF7ADD9AA28A922D2,RESELLER
    axonix.com,59163,RESELLER,bc385f2b4a87b721
    azberry.com,210126,RESELLER
    betweendigital.com,44851,RESELLER
    betweendigital.com,45117,RESELLER
    betweendigital.com,46214,RESELLER
    bidease.com,bidease_seller_28,RESELLER
    bidence.com,b8932c93bf4123ee6d969796302a037c,RESELLER
    bidmachine.io,124,RESELLER
    blis.com,74,RESELLER,61453ae19a4b73f4
    bold-win.com,555,RESELLER,71746737d0bab951
    castify.ai,1287331,RESELLER
    connekt.ai,8897271402,RESELLER
    consumable.com,2001583,RESELLER,aefcd3d2f45b5070
    consumable.com,2001585,RESELLER,aefcd3d2f45b5070
    contextweb.com,562329,RESELLER,89ff185a4c4e857c
    contextweb.com,562499,RESELLER,89ff185a4c4e857c
    contextweb.com,562615,RESELLER,89ff185a4c4e857c
    contextweb.com,562762,RESELLER,89ff185a4c4e857c
    contextweb.com,562794,RESELLER,89ff185a4c4e857c
    contextweb.com,562827,RESELLER,89ff185a4c4e857c
    contextweb.com,563321,RESELLER,89ff185a4c4e857c
    contextweb.com,563521,RESELLER,89ff185a4c4e857c
    contextweb.com,563539,RESELLER,89ff185a4c4e857c
    contextweb.com,563595,RESELLER,89ff185a4c4e857c
    contextweb.com,563604,RESELLER,89ff185a4c4e857c
    conversantmedia.com,100246,RESELLER,03113cd04947736d
    conversantmedia.com,100269,RESELLER,03113cd04947736d
    conversantmedia.com,100358,RESELLER,03113cd04947736d
    conversantmedia.com,100455,RESELLER,03113cd04947736d
    conversantmedia.com,100569,RESELLER,03113cd04947736d
    conversantmedia.com,100859,RESELLER,03113cd04947736d
    conversantmedia.com,100863,RESELLER,03113cd04947736d
    dauup.com,34101,RESELLER
    e-planning.net,75f09c55845dd6e3,RESELLER,c1ba615865ed87b2
    e-planning.net,79686787743ddfc5,RESELLER,c1ba615865ed87b2
    eskimi.com,2020000205,RESELLER
    flat-ads.com,206,RESELLER
    freewheel.tv,1137745,RESELLER
    freewheel.tv,1138513,RESELLER
    freewheel.tv,1601610,RESELLER
    gamaigroup.com,423058,RESELLER
    gitberry.com,345100012,RESELLER
    gitberry.com,375100011,RESELLER
    gumgum.com,15908,RESELLER,ffdef49475d318a9
    ignitemediatech.com,pub_11117,RESELLER
    iion.io,10192,RESELLER,013a29748465dc57
    improvedigital.com,2002,RESELLER
    improvedigital.com,2110,RESELLER
    improvedigital.com,2276,RESELLER
    improvedigital.com,2297,RESELLER
    improvedigital.com,2451,RESELLER
    improvedigital.com,2498,RESELLER
    improvedigital.com,2505,RESELLER
    improvedigital.com,2508,RESELLER
    improvedigital.com,2509,RESELLER
    inceptionmedia.ai,642690015,RESELLER
    indexexchange.com,191572,RESELLER
    indexexchange.com,194730,RESELLER
    inmobi.com,1e13a47050bc40e29ef24c145820180d,RESELLER,83e75a7ae333ca9d
    inmobi.com,23800006c42e4a739966c05ca0ac4854,RESELLER,83e75a7ae333ca9d
    inmobi.com,791b84bdd791470faa8dca5f04e6a83b,RESELLER,83e75a7ae333ca9d
    inmobi.com,7c9e112a810a4b639bb6af3fa397233e,RESELLER,83e75a7ae333ca9d
    inmobi.com,9e311c7a68e94888aac7fbb4272381e2,RESELLER,83e75a7ae333ca9d
    inmobi.com,ebc44eef6c104f7fbf7bc52ee29f7939,RESELLER,83e75a7ae333ca9d
    inmobi.com,edd282ac8f29464792bf2b7f3df2f9df,RESELLER,83e75a7ae333ca9d
    krushmedia.com,AJxF6R585a9M6CaTvK,RESELLER
    lijit.com,273644,RESELLER,fafdf38b16bf6b2b
    lijit.com,380632,RESELLER,fafdf38b16bf6b2b
    lijit.com,417620,RESELLER,fafdf38b16bf6b2b
    lijit.com,481366,RESELLER,fafdf38b16bf6b2b
    lijit.com,483304,RESELLER,fafdf38b16bf6b2b
    lijit.com,500520,RESELLER,fafdf38b16bf6b2b
    lijit.com,503707,RESELLER,fafdf38b16bf6b2b
    lijit.com,530498,RESELLER,fafdf38b16bf6b2b
    lijit.com,535224,RESELLER,fafdf38b16bf6b2b
    lijit.com,545922,RESELLER,fafdf38b16bf6b2b
    lijit.com,545924,RESELLER,fafdf38b16bf6b2b
    loopme.com,11362,RESELLER,6c8d5f95897a5a3b
    loopme.com,11367,RESELLER,6c8d5f95897a5a3b
    loopme.com,11426,RESELLER,6c8d5f95897a5a3b
    loopme.com,11463,RESELLER,6c8d5f95897a5a3b
    loopme.com,11605,RESELLER,6c8d5f95897a5a3b
    loopme.com,11635,RESELLER,6c8d5f95897a5a3b
    loopme.com,5176,RESELLER,6c8d5f95897a5a3b
    lunamedia.io,2zr5ys29xxoexdl,RESELLER,524ecb396915caaf
    markappmedia.site,C-1019,RESELLER
    mars.media,103116,RESELLER
    media.net,8CU12AQ1Y,RESELLER
    media.net,8CU43768M,RESELLER
    media.net,8CU7H33B7,RESELLER
    media.net,8CUAU7HF1,RESELLER
    media.net,8CUI89K0D,RESELLER
    media.net,8CUSC3UJ7,RESELLER
    mman.kr,31983,RESELLER
    my-cast.tv,986797,RESELLER
    myfeature.tv,18cVUEPSLWPyXYmMQwgw,RESELLER
    odeeo.io,134207762,RESELLER,36cfd73091d5c3fc
    olaex.biz,100008,RESELLER
    onetag.com,82e44d118b79600,RESELLER
    onetag.com,8668c16092f01e8,RESELLER
    onetag.com,89211985ede1484,RESELLER
    onetag.com,8bd233947b607f0,RESELLER
    onetag.com,8dd1b050e3a7e60,RESELLER
    onetag.com,8dd8de7f341d57e,RESELLER
    onetag.com,925c32ef718e9fe,RESELLER
    openx.com,537140488,RESELLER,6a698e2ec38604c6
    openx.com,537153564,RESELLER,6a698e2ec38604c6
    openx.com,540022851,RESELLER,6a698e2ec38604c6
    openx.com,540298543,RESELLER,6a698e2ec38604c6
    openx.com,540421297,RESELLER,6a698e2ec38604c6
    openx.com,540679900,RESELLER,6a698e2ec38604c6
    openx.com,540866936,RESELLER,6a698e2ec38604c6
    opera.com,pub6739463379776,RESELLER,55a0c5fd61378de3
    opera.com,pub9598692093632,RESELLER,55a0c5fd61378de3
    orangeclickmedia.com,C-1019,RESELLER
    outbrain.com,0023749a2264ea0429a71b54ac9ca0de9a,RESELLER
    outbrain.com,002d7f7ba0bd74452f2b155d0dfb5cd6c8,RESELLER
    outbrain.com,0083474e9e7092cfca87a3deb5c92450b6,RESELLER
    playdigo.com,2036,RESELLER,92011346d63d3c30
    playwire.com,1025119,RESELLER
    prequel.tv,517,RESELLER
    pubmatic.com,156517,RESELLER,5d62403b186f2ace
    pubmatic.com,157559,RESELLER,5d62403b186f2ace
    pubmatic.com,157800,RESELLER,5d62403b186f2ace
    pubmatic.com,157899,RESELLER,5d62403b186f2ace
    pubmatic.com,158060,RESELLER,5d62403b186f2ace
    pubmatic.com,158154,RESELLER,5d62403b186f2ace
    pubmatic.com,158291,RESELLER,5d62403b186f2ace
    pubmatic.com,159499,RESELLER,5d62403b186f2ace
    pubmatic.com,159668,RESELLER,5d62403b186f2ace
    pubmatic.com,160113,RESELLER,5d62403b186f2ace
    pubmatic.com,160145,RESELLER,5d62403b186f2ace
    pubmatic.com,160846,RESELLER,5d62403b186f2ace
    pubmatic.com,161136,RESELLER,5d62403b186f2ace
    pubmatic.com,161151,RESELLER,5d62403b186f2ace
    pubmatic.com,161162,RESELLER,5d62403b186f2ace
    pubmatic.com,161368,RESELLER,5d62403b186f2ace
    pubmatic.com,161372,RESELLER,5d62403b186f2ace
    pubmatic.com,161771,RESELLER,5d62403b186f2ace
    pubmatic.com,162161,RESELLER,5d62403b186f2ace
    pubmatic.com,162588,RESELLER,5d62403b186f2ace
    pubmatic.com,162968,RESELLER,5d62403b186f2ace
    pubmatic.com,162974,RESELLER,5d62403b186f2ace
    pubmatic.com,163758,RESELLER,5d62403b186f2ace
    pubmatic.com,164119,RESELLER,5d62403b186f2ace
    pubmatic.com,164452,RESELLER,5d62403b186f2ace
    pubmatic.com,164532,RESELLER,5d62403b186f2ace
    pubmatic.com,164562,RESELLER,5d62403b186f2ace
    pubmatic.com,165117,RESELLER,5d62403b186f2ace
    pubmatic.com,165157,RESELLER,5d62403b186f2ace
    pubmatic.com,165239,RESELLER,5d62403b186f2ace
    pubmatic.com,165307,RESELLER,5d62403b186f2ace
    pubmatic.com,165340,RESELLER,5d62403b186f2ace
    pubmatic.com,165864,RESELLER,5d62403b186f2ace
    pubmatic.com,165980,RESELLER,5d62403b186f2ace
    pubnative.net,1004796,RESELLER,d641df8625486a7b
    pubnative.net,1007055,RESELLER,d641df8625486a7b
    pubnative.net,1007974,RESELLER,d641df8625486a7b
    pubnative.net,1008379,RESELLER,d641df8625486a7b
    pubnative.net,1009046,RESELLER,d641df8625486a7b
    pubnative.net,1009988,RESELLER,d641df8625486a7b
    pubnative.net,1010067,RESELLER,d641df8625486a7b
    pubnative.net,1010068,RESELLER,d641df8625486a7b
    pubnative.net,1010069,RESELLER,d641df8625486a7b
    pubnative.net,1010070,RESELLER,d641df8625486a7b
    pubnative.net,1010071,RESELLER,d641df8625486a7b
    pubnative.net,1010072,RESELLER,d641df8625486a7b
    pubnative.net,1010073,RESELLER,d641df8625486a7b
    rhebus.works,3370572591,RESELLER
    rhebus.works,5000863388,RESELLER
    risecodes.com,6548dccf58df6d00010625d2,RESELLER
    risecodes.com,663cba61d3708c000172de04,RESELLER
    rubiconproject.com,13132,RESELLER,0bfd66d529a55807
    rubiconproject.com,15278,RESELLER,0bfd66d529a55807
    rubiconproject.com,15320,RESELLER,0bfd66d529a55807
    rubiconproject.com,16824,RESELLER,0bfd66d529a55807
    rubiconproject.com,16928,RESELLER,0bfd66d529a55807
    rubiconproject.com,17328,RESELLER,0bfd66d529a55807
    rubiconproject.com,17608,RESELLER,0bfd66d529a55807
    rubiconproject.com,18364,RESELLER,0bfd66d529a55807
    rubiconproject.com,20744,RESELLER,0bfd66d529a55807
    rubiconproject.com,22412,RESELLER,0bfd66d529a55807
    rubiconproject.com,24084,RESELLER,0bfd66d529a55807
    rubiconproject.com,24170,RESELLER,0bfd66d529a55807
    rubiconproject.com,24362,RESELLER,0bfd66d529a55807
    rubiconproject.com,24400,RESELLER,0bfd66d529a55807
    rubiconproject.com,24600,RESELLER,0bfd66d529a55807
    rubiconproject.com,24752,RESELLER,0bfd66d529a55807
    rubiconproject.com,25064,RESELLER,0bfd66d529a55807
    rubiconproject.com,25322,RESELLER,0bfd66d529a55807
    rubiconproject.com,25336,RESELLER,0bfd66d529a55807
    rubiconproject.com,25386,RESELLER,0bfd66d529a55807
    rubiconproject.com,25748,RESELLER,0bfd66d529a55807
    rubiconproject.com,25966,RESELLER,0bfd66d529a55807
    rubiconproject.com,25978,RESELLER,0bfd66d529a55807
    rubiconproject.com,26064,RESELLER,0bfd66d529a55807
    rubiconproject.com,26184,RESELLER,0bfd66d529a55807
    rubiconproject.com,26250,RESELLER,0bfd66d529a55807
    rubiconproject.com,26292,RESELLER,0bfd66d529a55807
    rubiconproject.com,26310,RESELLER,0bfd66d529a55807
    rubiconproject.com,26386,RESELLER,0bfd66d529a55807
    rubiconproject.com,26476,RESELLER,0bfd66d529a55807
    rubiconproject.com,26608,RESELLER,0bfd66d529a55807
    rubiconproject.com,26958,RESELLER,0bfd66d529a55807
    sabio.us,100032,RESELLER,96ed93aaa9795702
    screenil.com,554156,RESELLER
    sharethrough.com,6qlnf8SY,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,Bc6D1WHS,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,bXzzb1bR,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,DQQogebZ,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,K6tB2uM1,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,LC86m64o,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,OAW69Fon,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,pLL9cLon,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,r9B1MG7E,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,UbYLkrys,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,veuioj8F,RESELLER,d53b998a7bd4ecd2
    showheroes.com,6018,RESELLER
    showheroes.com,6031,RESELLER
    sipo-inc.com,658147,RESELLER
    smaato.com,1100056110,RESELLER,07bcf65f187117b4
    smaato.com,1100057444,RESELLER,07bcf65f187117b4
    smaato.com,1100058831,RESELLER,07bcf65f187117b4
    smartadserver.com,3020,RESELLER,060d053dcf45cbf3
    smartadserver.com,3817,RESELLER,060d053dcf45cbf3
    smartadserver.com,4140,RESELLER,060d053dcf45cbf3
    smartadserver.com,4284,RESELLER,060d053dcf45cbf3
    smartadserver.com,4343,RESELLER,060d053dcf45cbf3
    smartadserver.com,4456,RESELLER,060d053dcf45cbf3
    smartadserver.com,4457,RESELLER,060d053dcf45cbf3
    smartadserver.com,4467,RESELLER,060d053dcf45cbf3
    smartadserver.com,4568,RESELLER,060d053dcf45cbf3
    smartadserver.com,4610,RESELLER,060d053dcf45cbf3
    smartadserver.com,4625,RESELLER,060d053dcf45cbf3
    smartadserver.com,4673,RESELLER,060d053dcf45cbf3
    smartadserver.com,4762,RESELLER,060d053dcf45cbf3
    smartadserver.com,4880,RESELLER,060d053dcf45cbf3
    smartadserver.com,5015,RESELLER,060d053dcf45cbf3
    smartclip.net,27985,RESELLER
    smartivi.ai,357152,RESELLER
    smartyads.com,300112,RESELLER,fd2bde0ff2e62c5d
    sonobi.com,4a289cdd79,RESELLER,d1a215d9eb5aee9e
    sonobi.com,861be9c200,RESELLER,d1a215d9eb5aee9e
    sonobi.com,d2b13d1c36,RESELLER,d1a215d9eb5aee9e
    taurusx.com,80010,RESELLER
    taurusx.com,80609,RESELLER
    thebrave.io,1234585,RESELLER,c25b2154543746ac
    thebrave.io,1234598,RESELLER,c25b2154543746ac
    themediagrid.com,A6CWLO,RESELLER,35d5010d7789b49d
    themediagrid.com,A8X5S7,RESELLER,35d5010d7789b49d
    themediagrid.com,B8N9YH,RESELLER,35d5010d7789b49d
    themediagrid.com,IOY14K,RESELLER,35d5010d7789b49d
    themediagrid.com,OTJDBI,RESELLER,35d5010d7789b49d
    themediagrid.com,RABCQ1,RESELLER,35d5010d7789b49d
    themediagrid.com,RIX2M5,RESELLER,35d5010d7789b49d
    themediagrid.com,SWH94X,RESELLER,35d5010d7789b49d
    thunder-monetize.com,4977054380,RESELLER
    thunder-monetize.com,9550919845,RESELLER
    toponad.com,1658d4a00557ce,RESELLER,1d49fe424a1a456d
    triplelift.com,12158,RESELLER,6c33edb13117fd86
    triplelift.com,12240,RESELLER,6c33edb13117fd86
    triplelift.com,13042,RESELLER,6c33edb13117fd86
    triplelift.com,13043,RESELLER,6c33edb13117fd86
    triplelift.com,13907,RESELLER,6c33edb13117fd86
    triplelift.com,8614,RESELLER,6c33edb13117fd86
    triplelift.com,8844,RESELLER,6c33edb13117fd86
    trustedstack.com,TS5UCV3O4,RESELLER
    ucfunnel.com,par-E2B3A33EAA4286BEF7ADD9AA28A922D2,RESELLER
    undertone.com,4102,RESELLER
    verve.com,14592,RESELLER,0c8f5958fc2d6270
    video.unrulymedia.com,123476257,RESELLER
    video.unrulymedia.com,168269289,RESELLER
    video.unrulymedia.com,169764768,RESELLER
    video.unrulymedia.com,174356361,RESELLER
    video.unrulymedia.com,2464975885,RESELLER
    video.unrulymedia.com,322585001,RESELLER
    video.unrulymedia.com,649652654350370180,RESELLER
    video.unrulymedia.com,689559416,RESELLER
    video.unrulymedia.com,744858541,RESELLER
    video.unrulymedia.com,809865099,RESELLER
    video.unrulymedia.com,827123801,RESELLER
    video.unrulymedia.com,848837292,RESELLER
    video.unrulymedia.com,906352066,RESELLER
    videoheroes.tv,212428,RESELLER,064bc410192443d8
    VideoHeroes.tv,212504,RESELLER,064bc410192443d8
    vidoomy.com,4930225,RESELLER
    webeyemob.com,80010,RESELLER
    webeyemob.com,80609,RESELLER
    xad.com,241,RESELLER,81cbf0a75a5e0e9a
    xad.com,556,RESELLER,81cbf0a75a5e0e9a
    xandr.com,13293,RESELLER,f5ab79cb980f11d1
    xandr.com,13799,RESELLER,f5ab79cb980f11d1
    xandr.com,15278,RESELLER,f5ab79cb980f11d1
    xandr.com,15426,RESELLER,f5ab79cb980f11d1
    xandr.com,15769,RESELLER
    xandr.com,15840,RESELLER,f5ab79cb980f11d1
    yabbi.me,675d9254-4032-47ed-9074-b6ef4dbab522,RESELLER
    yeahmobi.com,111110,RESELLER
    yeahmobi.com,5135535,RESELLER
    yieldlab.net,15465081,RESELLER
    yieldmo.com,3591550211052216798,RESELLER,6a92c77cfc3d2258
    zetaglobal.net,815,RESELLER
    zetaglobal.net,856,RESELLER
    zmaticoo.com,5135655,RESELLER
    
    ironsrc.com, 247471, DIRECT
    
    e-planning.net, 79686787743ddfc5, RESELLER, c1ba615865ed87b2
    inmobi.com, b01aa06531c543d8a5fb9982f60afb00, RESELLER, 83e75a7ae333ca9d
    inmobi.com, edd282ac8f29464792bf2b7f3df2f9df, RESELLER, 83e75a7ae333ca9d
    inmobi.com, 7798aa742d014cd79627bb67ec431379, RESELLER, 83e75a7ae333ca9d
    inmobi.com, 0c8e054083954c68a45044396a1c3df8, RESELLER, 83e75a7ae333ca9d
    inmobi.com, 791b84bdd791470faa8dca5f04e6a83b, RESELLER, 83e75a7ae333ca9d
    orangeclickmedia.com, c-1181, RESELLER
    verve.com, 15503, RESELLER, 0c8f5958fc2d6270
    xandr.com, 7353, RESELLER
    xandr.com, 15769, RESELLER
    xandr.com, 13799, RESELLER
    onetag.com, 8668c16092f01e8, RESELLER
    onetag.com, 5d1628750185ace, RESELLER
    onetag.com, 8c90176af2e65c8, RESELLER
    openx.com, 540857594, RESELLER, 6a698e2ec38604c6
    openx.com, 540393169, RESELLER, 6a698e2ec38604c6
    openx.com, 540861995, RESELLER, 6a698e2ec38604c6
    openx.com, 540871654, RESELLER, 6a698e2ec38604c6
    openx.com, 540679900, RESELLER, 6a698e2ec38604c6
    openx.com, 541017750, RESELLER, 6a698e2ec38604c6
    risecodes.com, 6486c6155b231000010244b0, RESELLER
    pubwise.io, 22376824, RESELLER, c327c91a93a7cdd3
    rubiconproject.com, 15854, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 14558, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 18202, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 26270, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 25336, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 26292, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 26156, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 26132, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 24170, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 20744, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 20014, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 15268, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 22134, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 25978, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 24752, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 26250, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 24400, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 25872, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 24362, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 17608, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 26958, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 17130, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 26158, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 17960, RESELLER, 0bfd66d529a55807
    thunder-monetize.com, 5257091605, RESELLER
    thunder-monetize.com, 8221217003, RESELLER
    tradplusad.com, b3fca24bd11c1242, RESELLER
    video.unrulymedia.com, 585054906, RESELLER
    video.unrulymedia.com, 2464975885, RESELLER
    video.unrulymedia.com, 6111915185540403805, RESELLER
    video.unrulymedia.com, 123476257, RESELLER
    video.unrulymedia.com, 2743945877, RESELLER
    video.unrulymedia.com, 848837292, RESELLER
    video.unrulymedia.com, 436487936, RESELLER
    video.unrulymedia.com, 324806554, RESELLER, a670c89d4a324e47
    video.unrulymedia.com, 158727213, RESELLER
    video.unrulymedia.com, 755432070, RESELLER
    video.unrulymedia.com, 1058378980, RESELLER
    video.unrulymedia.com, 498216989, RESELLER
    zetaglobal.net, 815, RESELLER
    betweendigital.com, 45827, RESELLER
    betweendigital.com, 45329, RESELLER
    blueseasx.com, 232670, RESELLER, 7998eac5087f6110
    castify.ai, 1295184, RESELLER
    improvedigital.com, 2464, RESELLER
    improvedigital.com, 2525, RESELLER
    improvedigital.com, 1664, RESELLER
    improvedigital.com, 2276, RESELLER
    lacunads.com, lcb555b3bc8667a09a, RESELLER
    outbrain.com, 0023749a2264ea0429a71b54ac9ca0de9a, RESELLER
    outbrain.com, 00ab8f68679cc060c8bbc1035e70030614, RESELLER
    prado.co, 14260, RESELLER
    pubnative.net, 1007040, RESELLER, d641df8625486a7b
    pubnative.net, 1007974, RESELLER, d641df8625486a7b
    pubnative.net, 1008379, RESELLER, d641df8625486a7b
    pubnative.net, 1009988, RESELLER, d641df8625486a7b
    pubnative.net, 1006955, RESELLER, d641df8625486a7b
    connekt.ai, 1732945743, RESELLER
    sharethrough.com, 6qlnf8SY, RESELLER, d53b998a7bd4ecd2
    sharethrough.com, K5WDGAsP, RESELLER, d53b998a7bd4ecd2
    sharethrough.com, bXzzb1bR, RESELLER, d53b998a7bd4ecd2
    sharethrough.com, XeKuhSkz, RESELLER, d53b998a7bd4ecd2
    eliteappgrade.com, 494223, RESELLER
    onlinemediasolutions.com, 23120, RESELLER
    pokkt.com, 6246, RESELLER, c45702d9311e25fd
    smartclip.net, 27985, RESELLER
    admanmedia.com, 990, RESELLER
    admanmedia.com, 2153, RESELLER
    admanmedia.com, 2242, RESELLER
    ignitemediatech.com, pub_11115, RESELLER
    playwire.com, 1018531, RESELLER
    playwire.com, 1025119, RESELLER
    rhebus.works, 5000863388, RESELLER
    appads.in, 106246, RESELLER
    lunamedia.io, 2fdb16a1-5025-4c41-9b17-e01e47ebc8b4, RESELLER, 524ecb396915caaf
    opera.com, pub6794361378752, RESELLER, 55a0c5fd61378de3
    playdigo.com, 1965, RESELLER, 92011346d63d3c30
    vidazoo.com, 66e04f5e4d7fd89d878d29d2, RESELLER, b6ada874b4d7d0b2
    krushmedia.com, ajxf6R585a9m6Catvk, RESELLER
    videoheroes.tv, 212428, RESELLER, 064bc410192443d8
    videoheroes.tv, 212499, RESELLER, 064bc410192443d8
    webeyemob.com, 80029, RESELLER
    zmaticoo.com, 5135704, RESELLER
    zmaticoo.com, 5135195, RESELLER
    adform.com, 716, RESELLER
    appnexus.com, 15349, RESELLER, f5ab79cb980f11d1
    appnexus.com, 17234, RESELLER
    appnexus.com, 12878, RESELLER, f5ab79cb980f11d1
    appnexus.com, 15629, RESELLER
    appnexus.com, 9382, RESELLER, f5ab79cb980f11d1
    contextweb.com, 562791, RESELLER, 89ff185a4c4e857c
    contextweb.com, 563217, RESELLER, 89ff185a4c4e857c
    contextweb.com, 563265, RESELLER, 89ff185a4c4e857c
    contextweb.com, 562827, RESELLER, 89ff185a4c4e857c
    contextweb.com, 563079, RESELLER, 89ff185a4c4e857c
    mars.media, 1010442, RESELLER, 8624339f102fb076
    pubmatic.com, 158060, RESELLER, 5d62403b186f2ace
    pubmatic.com, 158481, RESELLER, 5d62403b186f2ace
    pubmatic.com, 159668, RESELLER, 5d62403b186f2ace
    pubmatic.com, 164601, RESELLER, 5d62403b186f2ace
    pubmatic.com, 161151, RESELLER, 5d62403b186f2ace
    pubmatic.com, 162882, RESELLER, 5d62403b186f2ace
    pubmatic.com, 156520, RESELLER, 5d62403b186f2ace
    pubmatic.com, 160493, RESELLER, 5d62403b186f2ace
    pubmatic.com, 158154, RESELLER, 5d62403b186f2ace
    pubmatic.com, 165116, RESELLER, 5d62403b186f2ace
    pubmatic.com, 164119, RESELLER, 5d62403b186f2ace
    pubmatic.com, 160456, RESELLER, 5d62403b186f2ace
    pubmatic.com, 162270, RESELLER, 5d62403b186f2ace
    pubmatic.com, 162588, RESELLER, 5d62403b186f2ace
    pubmatic.com, 165438, RESELLER, 5d62403b186f2ace
    pubmatic.com, 164532, RESELLER, 5d62403b186f2ace
    pubmatic.com, 162968, RESELLER, 5d62403b186f2ace
    pubmatic.com, 159906, RESELLER, 5d62403b186f2ace
    pubmatic.com, 163319, RESELLER, 5d62403b186f2ace
    pubmatic.com, 159501, RESELLER, 5d62403b186f2ace
    toponad.com, 165535463cfb70, RESELLER, 1d49fe424a1a456d
    yeahmobi.com, 5135195, RESELLER
    bid-matrix.com, f2c71a45ea149366, RESELLER
    eskimi.com, 2020000663, RESELLER
    eskimi.com, 2020000545, RESELLER
    google.com, pub-3990748024667386, RESELLER, f08c47fec0942fa0
    kidoz.net, 14260, RESELLER, a109366414b7335e
    ushareit.com, lcb555b3bc8667a09a, RESELLER
    voisetech.com, 1066, RESELLER
    conversantmedia.com, 100396, RESELLER, 03113cd04947736d
    conversantmedia.com, 100358, RESELLER, 03113cd04947736d
    conversantmedia.com, 100863, RESELLER, 03113cd04947736d
    media.net, 8CU12AQ1Y, RESELLER
    media.net, 8CU813E2H, RESELLER
    smartadserver.com, 4839, RESELLER, 060d053dcf45cbf3
    smartadserver.com, 4610, RESELLER, 060d053dcf45cbf3
    smartadserver.com, 4568, RESELLER, 060d053dcf45cbf3
    smartadserver.com, 3172, RESELLER, 060d053dcf45cbf3
    smartadserver.com, 4467, RESELLER
    triplelift.com, 12908, RESELLER, 6c33edb13117fd86
    triplelift.com, 13883, RESELLER, 6c33edb13117fd86
    triplelift.com, 12158, RESELLER, 6c33edb13117fd86
    grouprvn.com, 420212011, RESELLER
    lijit.com, 503707, RESELLER, fafdf38b16bf6b2b
    lijit.com, 511365, RESELLER, fafdf38b16bf6b2b
    lijit.com, 491737, RESELLER, fafdf38b16bf6b2b
    lijit.com, 546566, RESELLER, fafdf38b16bf6b2b
    lijit.com, 481366, RESELLER, fafdf38b16bf6b2b
    lijit.com, 483304, RESELLER, fafdf38b16bf6b2b
    lijit.com, 489963, RESELLER, fafdf38b16bf6b2b
    lijit.com, 465542, RESELLER, fafdf38b16bf6b2b
    lijit.com, 417620, RESELLER, fafdf38b16bf6b2b
    taurusx.com, 80029, RESELLER
    thebrave.io, 1234634, RESELLER, c25b2154543746ac
    
    
    unity.com, 5902657, DIRECT, 96cabb5fbdde37a7
    algorix.co, 54616, RESELLER, 5b394c12fea27a1d
    freewheel.tv, 741650, RESELLER
    loopme.com, 11463, RESELLER, 6c8d5f95897a5a3b
    loopme.com, 11635, RESELLER, 6c8d5f95897a5a3b
    loopme.com, 9621, RESELLER, 6c8d5f95897a5a3b
    visiblemeasures.com, 1020, RESELLER
    xapads.com, 117219, RESELLER
    smartadserver.com, 4998, RESELLER
    aceex.io, 1926, RESELLER, b1cf3c874d5c6682
    xandr.com, 8916, RESELLER
    pubmatic.com, 160318, RESELLER, 5d62403b186f2ace
    themediagrid.com, falino, DIRECT, 35d5010d7789b49d
    mango.mp, 323120, RESELLER
    undertone.com, 4108, RESELLER
    thebrave.io, 1234634, DIRECT, c25b2154543746ac
    betweendigital.com, 45827, DIRECT
    visoon.de, 64393, RESELLER
    brightcom.com, 23120, RESELLER
    admanmedia.com, 2037, RESELLER
    thebrave.io, 1234764, DIRECT, c25b2154543746ac
    xandr.com, 3318, RESELLER
    smartclip.net, 15458, RESELLER
    ushareit.com, lcb555b3bc8667a09a, DIRECT
    blueseasx.com, 203613, RESELLER, 7998eac5087f6110
    start.io, 116712987, RESELLER
    google.com, pub-5060663379040713, RESELLER, f08c47fec0942fa1
    app-stock.com, 256917, RESELLER
    sharethrough.com, ftvct81v, RESELLER
    criteo.com, b-057601, RESELLER, 9fac4a4a87c2a44f
    bidease.com, bidease_seller_20, RESELLER
    lacunads.com, lcb555b3bc8667a09a, DIRECT
    bidmachine.io, 162, RESELLER
    smartadserver.com, 4322, RESELLER
    connectad.io, 453, RESELLER, 85ac85a30c93b3e5
    risecodes.com, 66bca72035bff70001caa46f, RESELLER
    
    vungle.com,6306fa19a54be3001bb332f5,DIRECT,c107d686becd2d77
    Contextweb.com,562852,RESELLER,89ff185a4c4e857c
    Media.net,8CU12AQ1Y,RESELLER
    Media.net,8CU9B72O6,RESELLER
    acexchange.co.kr,1317287336,RESELLER
    acexchange.co.kr,1854269911,RESELLER
    adelement.com,30208,RESELLER
    adform.com,2671,RESELLER
    adform.com,2742,RESELLER
    adform.com,3083,RESELLER
    adform.com,3119,RESELLER,9f5210a2f0999e32
    adiiix.com,3d9335d3,RESELLER
    adiiix.com,db716a4c,RESELLER
    adingo.jp,31054,RESELLER
    admanmedia.com,2248,RESELLER
    admanmedia.com,990,RESELLER
    admixer.co.kr,1094,RESELLER
    admixer.co.kr,1610,RESELLER
    admixer.co.kr,1654,RESELLER
    admixer.net,5c5e6567-8486-42b9-9315-9262bd744b71,RESELLER
    adtiming.com,a-126,RESELLER,bf66753b8f380142
    adview.com,06667060,RESELLER,1b2cc038a11ea319
    adview.com,32076181,RESELLER,1b2cc038a11ea319
    advlion.com,3117,RESELLER
    adyoulike.com,76375ac0f5abb5a550c87326b9063b59,RESELLER
    adyoulike.com,78afbc34fac571736717317117dfa247,RESELLER
    adyoulike.com,994b7b4e03898048761c7110f11e56c2,RESELLER
    adyoulike.com,a2226c27fc2a6773f6a2b365e013513a,RESELLER
    adyoulike.com,e48120b055ac0abcc41c0093bf3fe02e,RESELLER
    adyoulike.com,f78b24039641337f06634ab9a3da4b48,RESELLER
    algorix.co,604633,RESELLER,5b394c12fea27a1d
    aniview.com,603f65a2e291680ef30af9c7,RESELLER,78b21b97965ec3f8
    app-stock.com,343264,RESELLER
    appnexus.com,11826,RESELLER
    appnexus.com,11924,RESELLER,f5ab79cb980f11d1
    appnexus.com,12700,RESELLER,f5ab79cb980f11d1
    appnexus.com,12878,RESELLER,f5ab79cb980f11d1
    appnexus.com,13293,RESELLER
    appnexus.com,13297,RESELLER,f5ab79cb980f11d1
    appnexus.com,15349,RESELLER,f5ab79cb980f11d1
    appnexus.com,15426,RESELLER,f5ab79cb980f11d1
    appnexus.com,15816,RESELLER,f5ab79cb980f11d1
    appnexus.com,17417,RESELLER,f5ab79cb980f11d1
    appnexus.com,1752,RESELLER,f5ab79cb980f11d1
    appnexus.com,4052,RESELLER
    appnexus.com,7351,RESELLER,f5ab79cb980f11d1
    appnexus.com,7597,RESELLER,f5ab79cb980f11d1
    appnexus.com,9316,RESELLER,f5ab79cb980f11d1
    appsheep.com,3a930d9b024c457d966d480b3421e3dd,RESELLER
    aralego.com,par-AA7B236BA32DD484C838E249362437B8,RESELLER
    aralego.com,par-D2346AAB7ABD36B4CDD7BBD264BA92E2,RESELLER
    aralego.com,par-E2B3A33EAA4286BEF7ADD9AA28A922D2,RESELLER
    azberry.com,321006,RESELLER
    bematterfull.com,22289765,RESELLER
    betweendigital.com,43843,RESELLER
    betweendigital.com,43860,RESELLER
    betweendigital.com,44098,RESELLER
    betweendigital.com,44917,RESELLER
    betweendigital.com,45270,RESELLER
    betweendigital.com,45656,RESELLER
    bid-matrix.com,7a33a9718c7ee898,RESELLER
    bidease.com,bidease_seller_31,RESELLER
    bidence.com,b8932c93bf4123ee6d969796302a037c,RESELLER
    bidence.com,d35302328a4b1f129d376f9c2932b0b7,RESELLER
    blueseasx.com,203592,RESELLER,7998eac5087f6110
    caerulus.io,3320-72775,RESELLER
    consumable.com,2001585,RESELLER,aefcd3d2f45b5070
    contextweb.com,558622,RESELLER,89ff185a4c4e857c
    contextweb.com,562546,RESELLER,89ff185a4c4e857c
    contextweb.com,562569,RESELLER,89ff185a4c4e857c
    contextweb.com,562615,RESELLER,89ff185a4c4e857c
    contextweb.com,562762,RESELLER,89ff185a4c4e857c
    contextweb.com,562791,RESELLER,89ff185a4c4e857c
    contextweb.com,562824,RESELLER,89ff185a4c4e857c
    contextweb.com,562827,RESELLER,89ff185a4c4e857c
    contextweb.com,563539,RESELLER,89ff185a4c4e857c
    contextweb.com,563601,RESELLER,89ff185a4c4e857c
    contextweb.com,563605,RESELLER,89ff185a4c4e857c
    conversantmedia.com,100242,RESELLER,03113cd04947736d
    conversantmedia.com,100246,RESELLER,03113cd04947736d
    conversantmedia.com,100269,RESELLER,03113cd04947736d
    conversantmedia.com,100308,RESELLER,03113cd04947736d
    conversantmedia.com,100358,RESELLER,03113cd04947736d
    conversantmedia.com,100396,RESELLER,03113cd04947736d
    conversantmedia.com,100863,RESELLER,03113cd04947736d
    conversantmedia.com,100886,RESELLER,03113cd04947736d
    copper6.com,764118,RESELLER
    criteo.com,B-074375,RESELLER,9fac4a4a87c2a44f
    display.io,173162,RESELLER
    e-planning.net,864690bcd16cd011,RESELLER,c1ba615865ed87b2
    eliteappgrade.com,494223,RESELLER
    equativ.com,4762,RESELLER,060d053dcf45cbf3
    equativ.com,5026,RESELLER,060d053dcf45cbf3
    freewheel.tv,1137745,RESELLER
    freewheel.tv,1138513,RESELLER
    freewheel.tv,1157729,RESELLER
    freewheel.tv,1157777,RESELLER
    freewheel.tv,1590601,RESELLER
    freewheel.tv,1590606,RESELLER
    fromthetop.io,65536,RESELLER
    gitberry.com,405100012,RESELLER
    gitberry.com,90scb,RESELLER
    google.com,pub-7214269347534569,RESELLER,f08c47fec0942fa0
    growintech.co,2723d092b63885e0d7c260cc007e8b9d2393g,RESELLER,9d8dfe5c6b00fb37
    hindsightsolutions.net,323-b5738,RESELLER,20e30b2ae1f670f2
    iion.io,10162,RESELLER
    improvedigital.com,1795,RESELLER
    improvedigital.com,2371,RESELLER
    improvedigital.com,2451,RESELLER
    improvedigital.com,2505,RESELLER
    improvedigital.com,2508,RESELLER
    improvedigital.com,2509,RESELLER
    improvedigital.com,2525,RESELLER
    improvedigital.com,2527,RESELLER
    improvedigital.com,2528,RESELLER
    improvedigital.com,2529,RESELLER
    improvedigital.com,2530,RESELLER
    improvedigital.com,2542,RESELLER
    inmobi.com,12a9a79d60214a40a444a6103b81747c,RESELLER,83e75a7ae333ca9d
    inmobi.com,22e5354e453f49348325184e25464adb,RESELLER,83e75a7ae333ca9d
    inmobi.com,23800006c42e4a739966c05ca0ac4854,RESELLER,83e75a7ae333ca9d
    inmobi.com,2ce1d976516340a49b12e59749659864,RESELLER,83e75a7ae333ca9d
    inmobi.com,38e36193f3c944d0b6254c71e511041b,RESELLER,83e75a7ae333ca9d
    inmobi.com,55049d2e109d4ac1820ca1432dda4e13,RESELLER,83e75a7ae333ca9d
    inmobi.com,5d2c913a0c5f4142acc7412723ee15d7,RESELLER,83e75a7ae333ca9d
    inmobi.com,867c89bb53994aaeb9dae3ce75b03e78,RESELLER,83e75a7ae333ca9d
    inmobi.com,9e311c7a68e94888aac7fbb4272381e2,RESELLER
    inmobi.com,ab915bcef5b24940bf745f1a8f427bec,RESELLER,83e75a7ae333ca9d
    inmobi.com,b76e9bc9a7d3407fa84ce458f01e313d,RESELLER,83e75a7ae333ca9d
    inmobi.com,c1e6d3502da64ebaa3ad0e4a4be15f11,RESELLER,83e75a7ae333ca9d
    inmobi.com,ddb41d8a9f434a918d05a0fc9999d9f9,RESELLER,83e75a7ae333ca9d
    inmobi.com,e2ce6fad847d4642b368082d662cc088,RESELLER,83e75a7ae333ca9d
    inmobi.com,ec6f6ceb8bb1440ba5455644ec96c275,RESELLER,83e75a7ae333ca9d
    inmobi.com,f3924290136e4129a5c082ff982c3a58,RESELLER,83e75a7ae333ca9d
    inmobi.com,f71b28e30a8249c3b08a9bca1a5cfbb7,RESELLER,83e75a7ae333ca9d
    iqzone.com,IQ39,RESELLER
    iqzone.com,IQ97,RESELLER
    kidoz.net,15127,RESELLER,a109366414b7335e
    leapmobs.com,38056,RESELLER
    lijit.com,273050,RESELLER,fafdf38b16bf6b2b
    lijit.com,400766,RESELLER,fafdf38b16bf6b2b
    lijit.com,417620,RESELLER,fafdf38b16bf6b2b
    lijit.com,481366,RESELLER,fafdf38b16bf6b2b
    lijit.com,488437,RESELLER,fafdf38b16bf6b2b
    lijit.com,500520,RESELLER,fafdf38b16bf6b2b
    lijit.com,503707,RESELLER,fafdf38b16bf6b2b
    lijit.com,512156,RESELLER,fafdf38b16bf6b2b
    lijit.com,512908,RESELLER,fafdf38b16bf6b2b
    lijit.com,530498,RESELLER,fafdf38b16bf6b2b
    lijit.com,543478,RESELLER,fafdf38b16bf6b2b
    lijit.com,545922,RESELLER,fafdf38b16bf6b2b
    lijit.com,545924,RESELLER,fafdf38b16bf6b2b
    loopme.com,10999,RESELLER,6c8d5f95897a5a3b
    loopme.com,11322,RESELLER,6c8d5f95897a5a3b
    loopme.com,11347,RESELLER,6c8d5f95897a5a3b
    loopme.com,11362,RESELLER,6c8d5f95897a5a3b
    loopme.com,11386,RESELLER,6c8d5f95897a5a3b
    loopme.com,11414,RESELLER,6c8d5f95897a5a3b
    loopme.com,2896,RESELLER,6c8d5f95897a5a3b
    loopme.com,5679,RESELLER,6c8d5f95897a5a3b
    loopme.com,9718,RESELLER,6c8d5f95897a5a3b
    lunamedia.io,95ed51edc1cb499baaf3476fcb6fa463,RESELLER,524ecb396915caaf
    mars.media,1010422,RESELLER,8624339f102fb076
    mars.media,107876,RESELLER,8624339f102fb076
    media.net,8CU76268D,RESELLER
    media.net,8CUI89K0D,RESELLER
    media.net,8CUN37DCC,RESELLER
    media.net,8CUP1N1F2,RESELLER
    media.net,8CUSC3UJ7,RESELLER
    meitu.com,755,RESELLER
    mintegral.com,10026,RESELLER
    mintegral.com,10030,RESELLER
    mman.kr,30769,RESELLER
    mman.kr,33085,RESELLER
    mman.kr,33476,RESELLER
    mobupps.com,c74d97b01eae257e44aa9d5bade97baf32954,RESELLER
    mobupps.com,c74d97b01eae257e44aa9d5bade97baf7726,RESELLER
    onairglobal.com,4357629,RESELLER
    onetag.com,8668c16092f01e8,RESELLER,0bfd66d529a55807
    onetag.com,8dd8de7f341d57e,RESELLER
    onlinemediasolutions.com,20444,RESELLER
    openx.com,537149888,RESELLER,6a698e2ec38604c6
    openx.com,537152826,RESELLER,6a698e2ec38604c6
    openx.com,539472296,RESELLER,6a698e2ec38604c6
    openx.com,540031703,RESELLER,6a698e2ec38604c6
    openx.com,540280728,RESELLER,6a698e2ec38604c6
    openx.com,540326226,RESELLER,6a698e2ec38604c6
    openx.com,540396775,RESELLER,6a698e2ec38604c6
    openx.com,540421297,RESELLER,6a698e2ec38604c6
    openx.com,540543195,RESELLER,6a698e2ec38604c6
    openx.com,540646187,RESELLER
    openx.com,540679900,RESELLER,6a698e2ec38604c6
    openx.com,540838151,RESELLER,6a698e2ec38604c6
    openx.com,540866936,RESELLER,6a698e2ec38604c6
    openx.com,541031350,RESELLER,6a698e2ec38604c6
    openx.com,544096208,RESELLER,6a698e2ec38604c6
    openx.com,561436793,RESELLER,6a698e2ec38604c6
    opera.com,pub4366595624320,RESELLER,55a0c5fd61378de3
    opera.com,pub4444433466368,RESELLER,55a0c5fd61378de3
    opera.com,pub5925993551616,RESELLER,55a0c5fd61378de3
    opera.com,pub6148735850944,RESELLER,55a0c5fd61378de3
    opera.com,pub9598692093632,RESELLER,55a0c5fd61378de3
    playdigo.com,1986,RESELLER,92011346d63d3c30
    playdigo.com,2041,RESELLER,92011346d63d3c30
    pokkt.com,5886,RESELLER,c45702d9311e25fd
    prado.co,15127,RESELLER
    prequel.tv,514,RESELLER
    programmaticx.ai,6888889,RESELLER
    pubmatic.com,156177,RESELLER,5d62403b186f2ace
    pubmatic.com,156425,RESELLER,5d62403b186f2ace
    pubmatic.com,156439,RESELLER,5d62403b186f2ace
    pubmatic.com,156517,RESELLER,5d62403b186f2ace
    pubmatic.com,156520,RESELLER,5d62403b186f2ace
    pubmatic.com,156631,RESELLER,5d62403b186f2ace
    pubmatic.com,156835,RESELLER,5d62403b186f2ace
    pubmatic.com,156931,RESELLER,5d62403b186f2ace
    pubmatic.com,157097,RESELLER,5d62403b186f2ace
    pubmatic.com,157559,RESELLER,5d62403b186f2ace
    pubmatic.com,157654,RESELLER,5d62403b186f2ace
    pubmatic.com,157800,RESELLER,5d62403b186f2ace
    pubmatic.com,158060,RESELLER,5d62403b186f2ace
    pubmatic.com,158100,RESELLER,5d62403b186f2ace
    pubmatic.com,158154,RESELLER,5d62403b186f2ace
    pubmatic.com,158408,RESELLER,5d62403b186f2ace
    pubmatic.com,158481,RESELLER,5d62403b186f2ace
    pubmatic.com,159035,RESELLER,5d62403b186f2ace
    pubmatic.com,159277,RESELLER
    pubmatic.com,159501,RESELLER,5d62403b186f2ace
    pubmatic.com,159668,RESELLER
    pubmatic.com,159846,RESELLER,5d62403b186f2ace
    pubmatic.com,160113,RESELLER,5d62403b186f2ace
    pubmatic.com,160145,RESELLER,5d62403b186f2ace
    pubmatic.com,160194,RESELLER,5d62403b186f2ace
    pubmatic.com,160195,RESELLER,5d62403b186f2ace
    pubmatic.com,160692,RESELLER,5d62403b186f2ace
    pubmatic.com,160846,RESELLER,5d62403b186f2ace
    pubmatic.com,160974,RESELLER,5d62403b186f2ace
    pubmatic.com,161136,RESELLER,5d62403b186f2ace
    pubmatic.com,161162,RESELLER,5d62403b186f2ace
    pubmatic.com,161267,RESELLER,5d62403b186f2ace
    pubmatic.com,161372,RESELLER,5d62403b186f2ace
    pubmatic.com,161748,RESELLER,5d62403b186f2ace
    pubmatic.com,162161,RESELLER,5d62403b186f2ace
    pubmatic.com,162223,RESELLER,5d62403b186f2ace
    pubmatic.com,162882,RESELLER,5d62403b186f2ace
    pubmatic.com,162974,RESELLER,5d62403b186f2ace
    pubmatic.com,163319,RESELLER,5d62403b186f2ace
    pubmatic.com,163476,RESELLER,5d62403b186f2ace
    pubmatic.com,164532,RESELLER,5d62403b186f2ace
    pubmatic.com,165117,RESELLER,5d62403b186f2ace
    pubmatic.com,165438,RESELLER,5d62403b186f2ace
    pubmatic.com,165980,RESELLER,5d62403b186f2ace
    pubmatic.com,166078,RESELLER,5d62403b186f2ace
    pubmatic.com,166409,RESELLER,5d62403b186f2ace
    pubnative.net,1007249,RESELLER,d641df8625486a7b
    pubnative.net,1007262,RESELLER,d641df8625486a7b
    pubnative.net,1007303,RESELLER,d641df8625486a7b
    pubnative.net,1007475,RESELLER,d641df8625486a7b
    pubnative.net,1007501,RESELLER,d641df8625486a7b
    pubnative.net,1009029,RESELLER,d641df8625486a7b
    pubnative.net,1009966,RESELLER,d641df8625486a7b
    pubnative.net,1009988,RESELLER,d641df8625486a7b
    pubnative.net,1010067,RESELLER,d641df8625486a7b
    pubnative.net,1010068,RESELLER,d641df8625486a7b
    pubnative.net,1010069,RESELLER,d641df8625486a7b
    pubnative.net,1010070,RESELLER,d641df8625486a7b
    pubnative.net,1010071,RESELLER,d641df8625486a7b
    pubnative.net,1010072,RESELLER,d641df8625486a7b
    pubnative.net,1010073,RESELLER,d641df8625486a7b
    pubwise.io,52324626,RESELLER,c327c91a93a7cdd3
    rhebus.works,4962256775,RESELLER
    rhebus.works,9176497567,RESELLER
    rhythmone.com,5336134699710583737,RESELLER,a670c89d4a324e47
    risecodes.com,662a26f1f3d42a00015ffbad,RESELLER
    rubiconproject.com,11726,RESELLER,0bfd66d529a55807
    rubiconproject.com,12186,RESELLER,0bfd66d529a55807
    rubiconproject.com,12266,RESELLER,0bfd66d529a55807
    rubiconproject.com,12556,RESELLER,0bfd66d529a55807
    rubiconproject.com,13132,RESELLER,0bfd66d529a55807
    rubiconproject.com,13856,RESELLER,0bfd66d529a55807
    rubiconproject.com,14558,RESELLER,0bfd66d529a55807
    rubiconproject.com,15044,RESELLER,0bfd66d529a55807
    rubiconproject.com,15268,RESELLER
    rubiconproject.com,15278,RESELLER,0bfd66d529a55807
    rubiconproject.com,16114,RESELLER,0bfd66d529a55807
    rubiconproject.com,16834,RESELLER,0bfd66d529a55807
    rubiconproject.com,17328,RESELLER,0bfd66d529a55807
    rubiconproject.com,17608,RESELLER,0bfd66d529a55807
    rubiconproject.com,18202,RESELLER,0bfd66d529a55807
    rubiconproject.com,18364,RESELLER,0bfd66d529a55807
    rubiconproject.com,20050,RESELLER,0bfd66d529a55807
    rubiconproject.com,20744,RESELLER,0bfd66d529a55807
    rubiconproject.com,23644,RESELLER,0bfd66d529a55807
    rubiconproject.com,23980,RESELLER,0bfd66d529a55807
    rubiconproject.com,24170,RESELLER,0bfd66d529a55807
    rubiconproject.com,24362,RESELLER,0bfd66d529a55807
    rubiconproject.com,24400,RESELLER,0bfd66d529a55807
    rubiconproject.com,24448,RESELLER,0bfd66d529a55807
    rubiconproject.com,24600,RESELLER,0bfd66d529a55807
    rubiconproject.com,24752,RESELLER,0bfd66d529a55807
    rubiconproject.com,25386,RESELLER,0bfd66d529a55807
    rubiconproject.com,25686,RESELLER,0bfd66d529a55807
    rubiconproject.com,25872,RESELLER,0bfd66d529a55807
    rubiconproject.com,25978,RESELLER,0bfd66d529a55807
    rubiconproject.com,26064,RESELLER,0bfd66d529a55807
    rubiconproject.com,26132,RESELLER,0bfd66d529a55807
    rubiconproject.com,26184,RESELLER,0bfd66d529a55807
    rubiconproject.com,26292,RESELLER,0bfd66d529a55807
    rubiconproject.com,26846,RESELLER,0bfd66d529a55807
    saharmedia.net,1683858,RESELLER
    sharethrough.com,4762,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,5026,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,6qlnf8SY,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,Bc6D1WHS,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,DQQogebZ,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,NIRSC9f2,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,UbYLkrys,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,XeKuhSkz,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,ZqrIAHsE,RESELLER,d53b998a7bd4ecd2
    sharethrough.com,bXzzb1bR,RESELLER,d53b998a7bd4ecd2
    showheroes.com,6031,RESELLER
    showheroes.com,6037,RESELLER
    silvermob.com,419,RESELLER
    singularads.com,548745,RESELLER
    smaato.com,1100004890,RESELLER,07bcf65f187117b4
    smaato.com,1100044045,RESELLER,07bcf65f187117b4
    smaato.com,1100049757,RESELLER,07bcf65f187117b4
    smaato.com,1100051149,RESELLER,07bcf65f187117b4
    smaato.com,1100052485,RESELLER,07bcf65f187117b4
    smaato.com,1100056110,RESELLER,07bcf65f187117b4
    smaato.com,1100058954,RESELLER,07bcf65f187117b4
    smartadserver.com,1692,RESELLER
    smartadserver.com,3117,RESELLER
    smartadserver.com,3232,RESELLER
    smartadserver.com,3713,RESELLER
    smartadserver.com,3797,RESELLER
    smartadserver.com,3817,RESELLER
    smartadserver.com,4111,RESELLER
    smartadserver.com,4140,RESELLER
    smartadserver.com,4210,RESELLER,060d053dcf45cbf3
    smartadserver.com,4284,RESELLER
    smartadserver.com,4343,RESELLER
    smartadserver.com,4383,RESELLER
    smartadserver.com,4456,RESELLER,060d053dcf45cbf3
    smartadserver.com,4457,RESELLER,060d053dcf45cbf3
    smartadserver.com,4467,RESELLER
    smartadserver.com,4569,RESELLER
    smartadserver.com,4625,RESELLER,060d053dcf45cbf3
    smartadserver.com,4762,RESELLER,060d053dcf45cbf3
    smartadserver.com,4865,RESELLER
    smartadserver.com,4880,RESELLER,060d053dcf45cbf3
    smartadserver.com,5026,RESELLER,060d053dcf45cbf3
    smartclip.net,14275,RESELLER
    smartclip.net,27985,RESELLER
    smartstream.tv,679,RESELLER
    smartyads.com,100115,RESELLER,fd2bde0ff2e62c5d
    smartyads.com,100149,RESELLER,fd2bde0ff2e62c5d
    smartyads.com,368,RESELLER
    sonobi.com,2b51f34067,RESELLER,d1a215d9eb5aee9e
    sonobi.com,4a289cdd79,RESELLER,d1a215d9eb5aee9e
    sonobi.com,5b003395a5,RESELLER,d1a215d9eb5aee9e
    sonobi.com,7b37f8ccbc,RESELLER,d1a215d9eb5aee9e
    sonobi.com,a257177742,RESELLER,d1a215d9eb5aee9e
    sonobi.com,b43e9530e7,RESELLER,d1a215d9eb5aee9e
    sonobi.com,cddceeb06e,RESELLER,d1a215d9eb5aee9e
    sonobi.com,d2b13d1c36,RESELLER,d1a215d9eb5aee9e
    sovrn.com,500520,RESELLER,fafdf38b16bf6b2b
    spinx.biz,1373698863,RESELLER
    ssp.e-volution.ai,AJxF6R118a9M6CaTvK,RESELLER
    ssp.e-volution.ai,AJxF6R189a9M6CaTvK,RESELLER
    ssp.e-volution.ai,AJxF6R396a9M6CaTvK,RESELLER
    ssp.zetaglobal.net,815,RESELLER
    start.io,162328800,RESELLER
    target.my.com,8676470,RESELLER
    targetspot.com,297,RESELLER,feb28ed826dcf532
    taurusx.com,80066,RESELLER
    taurusx.com,80609,RESELLER
    thebrave.io,1234632,RESELLER,c25b2154543746ac
    thebrave.io,1234633,RESELLER,c25b2154543746ac
    thebrave.io,9840732,RESELLER,c25b2154543746ac
    themediagrid.com,1283WV,RESELLER,35d5010d7789b49d
    themediagrid.com,A8X5S7,RESELLER,35d5010d7789b49d
    themediagrid.com,DY1OVP,RESELLER,35d5010d7789b49d
    themediagrid.com,SWH94X,RESELLER,35d5010d7789b49d
    themediagrid.com,SYMTFD,RESELLER,35d5010d7789b49d
    thunder-monetize.com,2297825074,RESELLER
    thunder-monetize.com,6343837988,RESELLER
    tpmn.io,532,RESELLER
    tradplusad.com,97f87d32cbc437a3,RESELLER
    triplelift.com,10223,RESELLER,6c33edb13117fd86
    triplelift.com,11656,RESELLER,6c33edb13117fd86
    triplelift.com,12908,RESELLER,6c33edb13117fd86
    triplelift.com,13043,RESELLER,6c33edb13117fd86
    trustedstack.com,TS5UCV3O4,RESELLER
    tubia.com,40104,RESELLER
    ucfunnel.com,par-AA7B236BA32DD484C838E249362437B8,RESELLER
    ucfunnel.com,par-D2346AAB7ABD36B4CDD7BBD264BA92E2,RESELLER
    ucfunnel.com,par-E2B3A33EAA4286BEF7ADD9AA28A922D2,RESELLER
    unrulymedia.com,2464975885,RESELLER
    unrulymedia.com,689559416,RESELLER
    verve.com,14619,RESELLER,0c8f5958fc2d6270
    verve.com,15503,RESELLER,0c8f5958fc2d6270
    video.unrulymedia.com,169764768,RESELLER
    video.unrulymedia.com,2099898025,RESELLER
    video.unrulymedia.com,2613193077,RESELLER
    video.unrulymedia.com,3634565696,RESELLER,6f752381ad5ec0e5
    video.unrulymedia.com,3704396951,RESELLER
    video.unrulymedia.com,3855144660485329163,RESELLER
    video.unrulymedia.com,3881266972,RESELLER
    video.unrulymedia.com,3948367200,RESELLER
    video.unrulymedia.com,4201299756,RESELLER
    video.unrulymedia.com,4268206200,RESELLER
    video.unrulymedia.com,4631344382657206988,RESELLER
    video.unrulymedia.com,5127265468568773826,RESELLER
    video.unrulymedia.com,5336134699710583737,RESELLER
    video.unrulymedia.com,557688749,RESELLER
    video.unrulymedia.com,689559416,RESELLER
    video.unrulymedia.com,755432070,RESELLER
    video.unrulymedia.com,827123801,RESELLER
    video.unrulymedia.com,906352066,RESELLER
    video.unrulymedia.com,947824792,RESELLER
    videoheroes.tv,212428,RESELLER,064bc410192443d8
    videoheroes.tv,212450,RESELLER,064bc410192443d8
    videoheroes.tv,212459,RESELLER,064bc410192443d8
    visiblemeasures.com,1020,RESELLER
    webeyemob.com,80609,RESELLER
    xad.com,241,RESELLER,81cbf0a75a5e0e9a
    xandr.com,10128,RESELLER,f5ab79cb980f11d1
    xandr.com,12745,RESELLER,f5ab79cb980f11d1
    xandr.com,13238,RESELLER,f5ab79cb980f11d1
    xandr.com,13293,RESELLER,f5ab79cb980f11d1
    xandr.com,13297,RESELLER,f5ab79cb980f11d1
    xandr.com,13799,RESELLER
    xandr.com,15426,RESELLER,f5ab79cb980f11d1
    xandr.com,15840,RESELLER
    xandr.com,17417,RESELLER,f5ab79cb980f11d1
    yeahmobi.com,114122,RESELLER
    yieldmo.com,2754490424016969782,RESELLER
    yieldmo.com,3591550211052216798,RESELLER,6a92c77cfc3d2258
    yieldmo.com,3761877318507634863,RESELLER,6a92c77cfc3d2258
    zetaglobal.com,815,RESELLER
    zetaglobal.net,815,RESELLER
    zetaglobal.net,998,RESELLER
    zmaticoo.com,114490,RESELLER
    zmaticoo.com,5135063,RESELLER
    zmaticoo.com,5135224,RESELLER
    zoomd.com,88478941,RESELLER
    
    
    pangleglobal.com, 5005126, DIRECT
    pubmatic.com, 161490, RESELLER, 5d62403b186f2ace
    
    
    inmobi.com, 2b8d3da2a3774823a7f561cf7facfbb0, DIRECT, 83e75a7ae333ca9d
    inmobi.com, 2b8d3da2a3774823a7f561cf7facfbb0, DIRECT, 83e75a7ae333ca9d
    rubiconproject.com, 11726, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 12266, RESELLER, 0bfd66d529a55807
    loopme.com, 9724, RESELLER, 6c8d5f95897a5a3b
    rubiconproject.com, 20744, RESELLER, 0bfd66d529a55807
    openx.com, 540298543, RESELLER, 6a698e2ec38604c6
    contextweb.com, 562499, RESELLER, 89ff185a4c4e857c
    mintegral.com, 10003, RESELLER
    video.unrulymedia.com, 322585001, RESELLER
    rhebus.works, 7597018658, RESELLER
    blis.com, 33, RESELLER, 61453ae19a4b73f4
    algorix.co, 54190, RESELLER, 5b394c12fea27a1d
    pubmatic.com, 157097, RESELLER, 5d62403b186f2ace
    pubnative.net, 1006951, RESELLER, d641df8625486a7b
    yeahmobi.com, 5135082, RESELLER
    zmaticoo.com, 5135082, RESELLER
    zmaticoo.com, 113149, RESELLER
    opera.com, pub6871903319744, RESELLER, 55a0c5fd61378de3
    conversantmedia.com, 40881, RESELLER, 03113cd04947736d
    outbrain.com, 00bba279fec6daa01a0cb6fdccb023f0d5, RESELLER
    playwire.com, 1025119, RESELLER
    lijit.com, 502742, RESELLER, fafdf38b16bf6b2b
    inmobi.com, 2b8d3da2a3774823a7f561cf7facfbb0, DIRECT, 83e75a7ae333ca9d
    rubiconproject.com, 11726, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 12266, RESELLER, 0bfd66d529a55807
    loopme.com, 9724, RESELLER, 6c8d5f95897a5a3b
    rubiconproject.com, 20744, RESELLER, 0bfd66d529a55807
    openx.com, 540298543, RESELLER, 6a698e2ec38604c6
    contextweb.com, 562499, RESELLER, 89ff185a4c4e857c
    mintegral.com, 10003, RESELLER
    video.unrulymedia.com, 322585001, RESELLER
    rhebus.works, 7597018658, RESELLER
    blis.com, 33, RESELLER, 61453ae19a4b73f4
    algorix.co, 54190, RESELLER, 5b394c12fea27a1d
    pubmatic.com, 157097, RESELLER, 5d62403b186f2ace
    pubnative.net, 1006951, RESELLER, d641df8625486a7b
    yeahmobi.com, 5135082, RESELLER
    zmaticoo.com, 5135082, RESELLER
    zmaticoo.com, 113149, RESELLER
    opera.com, pub6871903319744, RESELLER, 55a0c5fd61378de3
    conversantmedia.com, 40881, RESELLER, 03113cd04947736d
    outbrain.com, 00bba279fec6daa01a0cb6fdccb023f0d5, RESELLER
    playwire.com, 1025119, RESELLER
    lijit.com, 502742, RESELLER, fafdf38b16bf6b2b
    9dotsmedia.com, 122262, RESELLER, 45ff185b4c4e857d
    adingenious.com, 75036, RESELLER
    admanmedia.com, 2063, RESELLER
    adtonos.com, PUB2356156682, RESELLER
    app-stock.com, 315417, RESELLER
    pubmatic.com, 163758, RESELLER, 5d62403b186f2ace
    rubiconproject.com, 16928, RESELLER, 0bfd66d529a55807
    bold-win.com, 148, RESELLER, 71746737d0bab951
    thebrave.io, 1234568, RESELLER, c25b2154543746ac
    onlinemediasolutions.com, 29556, RESELLER
    carbonatix.com, 141073554, RESELLER
    castify.ai, 1279123, RESELLER
    rubiconproject.com, 20050, RESELLER, 0bfd66d529a55807
    lijit.com, 481366, RESELLER, fafdf38b16bf6b2b
    peak226.com, 12904, RESELLER
    dauup.com, 34103, RESELLER
    eliteappgrade.com, 514554, RESELLER
    eskimi.com, 2020000030, RESELLER
    ssp.e-volution.ai, AJxF6R378a9M6CaTvK, RESELLER
    fromthetop.io, 65529, RESELLER
    gamaigroup.com, 320201, RESELLER
    algorix.co, 60367, RESELLER, 5b394c12fea27a1d
    growintech.co, 102275, RESELLER
    iion.io, 10171, RESELLER, 013a29748465dc57
    iqzone.com, IQ87, RESELLER
    keenkale.com, 841, RESELLER
    lemmatechnologies.com, 89, RESELLER, 7829010c5bebd1fb
    e-planning.net, 608359bb987625b2, RESELLER, c1ba615865ed87b2
    lunamedia.io, b82743ba2ddf4bcaab3d1d4f91b1330e, RESELLER, 524ecb396915caaf
    markappmedia.site, 587286, RESELLER
    orangeclickmedia.com, C-1034, RESELLER
    mars.media, 1010450, RESELLER, 8624339f102fb076
    bematterfull.com, 111399861, RESELLER
    criteo.com, B-057955, RESELLER, 9fac4a4a87c2a44f
    loopme.com, 11414, RESELLER, 6c8d5f95897a5a3b
    acexchange.co.kr, 1562884725, RESELLER
    prequel.tv, 518, RESELLER
    programmaticx.ai, 6999766, RESELLER
    pubmatic.com, 156931, RESELLER, 5d62403b186f2ace
    pubmatic.com, 159035, RESELLER, 5d62403b186f2ace
    appnexus.com, 8178, RESELLER, f5ab79cb980f11d1
    advertising.com, 28246, RESELLER
    onetag.com, 59aa7be4921bac8, RESELLER
    contextweb.com, 558638, RESELLER, 89ff185a4c4e857c
    rubiconproject.com, 26064, RESELLER, 0bfd66d529a55807
    video.unrulymedia.com, 188404962, RESELLER
    sabio.us, 100071, RESELLER, 96ed93aaa9795702
    rubiconproject.com, 26168, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 26192, RESELLER, 0bfd66d529a55807
    se7en.es, 212430, RESELLER, 064bc410192443d8
    videoheroes.tv, 212430, RESELLER, 064bc410192443d8
    smartadserver.com, 3232, RESELLER, 060d053dcf45cbf3
    smartyads.com, 478, RESELLER, fd2bde0ff2e62c5d
    sonobi.com, 45dd948092, RESELLER, d1a215d9eb5aee9e
    rubiconproject.com, 24400, RESELLER, 0bfd66d529a55807
    pubmatic.com, 157559, RESELLER, 5d62403b186f2ace
    tredio.io, 357a6fdf7642bf815a88822c447d9dc432901, RESELLER
    verve.com, 5897, RESELLER, 0c8f5958fc2d6270
    voisetech.com, 1023, RESELLER
    pubmatic.com, 165116, RESELLER, 5d62403b186f2ace
    freewheel.tv, 1599106, RESELLER
    freewheel.tv, 1599109, RESELLER
    thunder-monetize.com, 3905806597, RESELLER
    themediasense.com, 1147, RESELLER
    mobismarter.com, 3294, RESELLER
    mman.kr, 32102, RESELLER
    
    
    applovin.com, 0b6cd03676ee7b8a8c6e26598c9d1faa, DIRECT
    adyoulike.com, 2d578cf695c89ec2ba707280319d2020, RESELLER
    algorix.co, 60532, RESELLER, 5b394c12fea27a1d
    contextweb.com , 562615, RESELLER, 89ff185a4c4e857c
    gamaigroup.com, 423056, RESELLER
    improvedigital.com, 2451, RESELLER
    indexexchange.com, 211341, RESELLER, 50b1c356f2c5c8fc
    lijit.com, 516975, RESELLER, fafdf38b16bf6b2b
    loopme.com, 11303, RESELLER, 6c8d5f95897a5a3b
    loopme.com, 11424, RESELLER, 6c8d5f95897a5a3b
    media.net, 8CU12AQ1Y, RESELLER
    media.net, 8CUD06Z68, RESELLER
    openx.com, 540785403, RESELLER, 6a698e2ec38604c6
    openx.com, 559798512, RESELLER, 6a698e2ec38604c6
    opera.com, pub6538436382208, RESELLER, 55a0c5fd61378de3
    pubmatic.com, 158154, RESELLER, 5d62403b186f2ace
    pubmatic.com, 158862, RESELLER, 5d62403b186f2ace
    pubmatic.com, 159509, RESELLER, 5d62403b186f2ace
    pubmatic.com, 160974, RESELLER, 5d62403b186f2ace
    pubmatic.com, 164532, RESELLER, 5d62403b186f2ace
    pubnative.net, 1007501, RESELLER, d641df8625486a7b
    rhebus.works, 9019999555, RESELLER
    risecodes.com, 64e346c45ab8e700016b45cd, RESELLER
    rubiconproject.com, 16356, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 20050, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 20744, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 23876, RESELLER, 0bfd66d529a55807
    sharethrough.com, bXzzb1bR, RESELLER, d53b998a7bd4ecd2
    sharethrough.com, NNB0Zl76, RESELLER, d53b998a7bd4ecd2
    sharethrough.com, UbYLkrys, RESELLER, d53b998a7bd4ecd2
    smartadserver.com, 3817, RESELLER
    sonobi.com, d2b13d1c36, RESELLER, d1a215d9eb5aee9e
    video.unrulymedia.com, 169764768, RESELLER
    video.unrulymedia.com, 322585001, RESELLER
    xandr.com, 13799, RESELLER
    xandr.com, 14082, RESELLER
    yieldmo.com, 3591550211052216798, RESELLER, 6a92c77cfc3d2258
    zmaticoo.com, 5135655, RESELLER
    
    
    mintegral.com, 42575, DIRECT, 0aeed750c80d6423
    pokkt.com, 7139, RESELLER, c45702d9311e25fd
    appads.in, 107139, RESELLER
    pubmatic.com, 160456, RESELLER, 5d62403b186f2ace
    pubmatic.com, 156520, RESELLER, 5d62403b186f2ace
    criteo.com, B-057955, RESELLER, 9fac4a4a87c2a44f
    bigo.sg, 130, RESELLER
    toponad.com, 16385e313a271a, DIRECT, 1d49fe424a1a456d
    pubmatic.com, 161151, RESELLER, 5d62403b186f2ace
    ignitemediatech.com, pub_61122, RESELLER
    ignitemediatech.com, 61119, RESELLER
    pubnative.net, 1007974, RESELLER, d641df8625486a7b
    verve.com, 15503, RESELLER, 0c8f5958fc2d6270
    lkqd.net, 647, RESELLER, 59c49fa9598a0117
    lkqd.net, 654, RESELLER, 59c49fa9598a0117
    conversantmedia.com, 100097, RESELLER, 03113cd04947736d
    indexexchange.com, 192829, RESELLER, 50b1c356f2c5c8fc
    bidmachine.io, 138, RESELLER
    indexexchange.com, 194730, RESELLER,50b1c356f2c5c8fc
    inmobi.com, ebc44eef6c104f7fbf7bc52ee29f7939, RESELLER, 83e75a7ae333ca9d
    adcolony.com, 7d04a18a58085918, RESELLER, 1ad675c9de6b5176
    contextweb.com, 562499, RESELLER, 89ff185a4c4e857c
    pokkt.com, 7606, RESELLER, c45702d9311e25fd
    appads.in, 107606, RESELLER
    admanmedia.com, 894, RESELLER
    rubiconproject.com, 14558, RESELLER, 0bfd66d529a55807
    contextweb.com, 561913, RESELLER, 89ff185a4c4e857c
    pubmatic.com, 158481, RESELLER, 5d62403b186f2ace
    vidoomy.com,7646534,DIRECT
    smartadserver.com, 3713, RESELLER
    axonix.com, 57869, RESELLER
    conversantmedia.com, 100308, RESELLER, 03113cd04947736d
    adform.com, 2671, RESELLER
    freewheel.tv, 1235183, RESELLER
    freewheel.tv, 1235279, RESELLER
    openx.com, 540866936, RESELLER, 6a698e2ec38604c6
    yieldmo.com, 2807970144171533194, RESELLER
    xandr.com, 8804, RESELLER, f5ab79cb980f11d1
    criteo.com, B-063105, RESELLER, 9fac4a4a87c2a44f
    indexexchange.com, 191497, RESELLER,50b1c356f2c5c8fc
    sonobi.com, 7b37f8ccbc, RESELLER, d1a215d9eb5aee9e
    inmobi.com, 3a4f7da341dd490cbb7dde02b126275e, RESELLER, 83e75a7ae333ca9d
    video.unrulymedia.com, 3948367200, RESELLER
    rhythmone.com, 3948367200, RESELLER, a670c89d4a324e47
    adx-dre.op.hicloud.com, PUB_HW_1003, RESELLER
    adx-dra.op.hicloud.com, PUB_HW_1003, RESELLER
    adx-drru.op.hicloud.com, PUB_HW_1003, RESELLER
    bidmachine.io, 114, RESELLER
    bidmachine.io, 67, RESELLER
    liftoff.io, 7f6945815e6, RESELLER
    hyperad.tech, 20, RESELLER
    adelement.com, 101, RESELLER
    advertising.com, 28886, RESELLER
    pubmatic.com, 157558, RESELLER, 5d62403b186f2ace
    pubmatic.com, 158362, RESELLER, 5d62403b186f2ace
    pubmatic.com, 156855, RESELLER, 5d62403b186f2ace
    appnexus.com, 11450, RESELLER, f5ab79cb980f11d1
    appnexus.com, 13181, RESELLER, f5ab79cb980f11d1
    pubmatic.com, 160058, RESELLER, 5d62403b186f2ace
    rubiconproject.com, 23822, RESELLER, 0bfd66d529a55807
    smartadserver.com, 4202, RESELLER
    opera.com, pub5881703796480, RESELLER, 55a0c5fd61378de3
    betweendigital.com, 44267, RESELLER
    yeahmobi.com, 5135315, RESELLER
    smartyads.com, 100114, RESELLER, fd2bde0ff2e62c5d
    adelement.com, 20056, RESELLER
    adelement.com, 20210, RESELLER
    adelement.com, 20029, RESELLER
    adelement.com, 30018, RESELLER
    adelement.com, 30019, RESELLER
    loopme.com, 11442, RESELLER, 6c8d5f95897a5a3b
    admanmedia.com, 613, RESELLER
    appnexus.com, 12061, RESELLER, f5ab79cb980f11d1
    appnexus.com, 12501, RESELLER, f5ab79cb980f11d1
    thebrave.io, 9840732, RESELLER, c25b2154543746ac
    smartadserver.com, 3964, RESELLER
    appnexus.com, 12878, RESELLER, f5ab79cb980f11d1
    indexexchange.com, 194974, RESELLER, 50b1c356f2c5c8fc
    mediafuse.com, 316569, RESELLER
    e-planning.net, af5a107262468954, RESELLER, c1ba615865ed87b2
    nobid.io, 22575233870, RESELLER
    sonobi.com, 2b51f34067, RESELLER, d1a215d9eb5aee9e
    ssp.e-volution.ai, AJxF6R119a9M6CaTvK, RESELLER
    axonix.com, 59054, RESELLER, bc385f2b4a87b721
    programmaticx.ai, 6845438, RESELLER
    pubmatic.com, 161018, RESELLER, 5d62403b186f2ace
    ssp.logan.ai, LG15, RESELLER
    appnexus.com, 12964, RESELLER, f5ab79cb980f11d1
    mars.media, 1010380, RESELLER, 8624339f102fb076
    pubnative.net, 1007063, RESELLER, d641df8625486a7b
    pubnative.net, 1003601, RESELLER, d641df8625486a7b
    pubmatic.com, 160145, RESELLER, 5d62403b186f2ace
    rubiconproject.com, 15278, RESELLER, 0bfd66d529a55807
    sabio.us, 100032, RESELLER, 96ed93aaa9795702
    loopme.com, 11322, RESELLER, 6c8d5f95897a5a3b
    improvedigital.com, 1210, RESELLER
    appnexus.com, 13091, RESELLER, f5ab79cb980f11d1
    emxdgt.com, 2041, RESELLER, 1e1d41537f7cad7f
    mars.media, 1010421, RESELLER, 8624339f102fb076
    appnexus.com, 12223, RESELLER, f5ab79cb980f11d1
    pubmatic.com, 160194, RESELLER, 5d62403b186f2ace
    gitberry.com, 3OJ, RESELLER
    smartadserver.com, 4039, RESELLER
    appnexus.com, 13074, RESELLER, f5ab79cb980f11d1
    inmobi.com, 5baa7ca93ef847c0876297e737dac3ee, RESELLER, 83e75a7ae333ca9d
    conversantmedia.com, 100293, RESELLER, 03113cd04947736d
    aol.com, 59025, RESELLER, e1a5b5b6e3255540
    yahoo.com, 59025, RESELLER, e1a5b5b6e3255540
    castify.ai, 1210421, RESELLER
    indexexchange.com, 195812, RESELLER, 50b1c356f2c5c8fc
    se7en.es, 212450, RESELLER, 064bc410192443d8
    videoheroes.tv, 212450, RESELLER, 064bc410192443d8
    themediagrid.com, B8N9YH, RESELLER, 35d5010d7789b49d
    appnexus.com, 12976, RESELLER, f5ab79cb980f11d1
    sonobi.com, ac6f1a9ded, RESELLER, d1a215d9eb5aee9e
    nobid.io, 22388449880, RESELLER
    betweendigital.com, 43740, RESELLER
    onetag.com, 601a5990b7f5cf7, RESELLER
    e-planning.net, ea9874674191f1ff, RESELLER, c1ba615865ed87b2
    betweendigital.com, 44098, RESELLER
    openx.com, 541177349, RESELLER, 6a698e2ec38604c6
    adcolony.com, 29b7f4a14dc689eb, RESELLER, 1ad675c9de6b5176
    meitu.com, 654, RESELLER
    sovrn.com, 273644, RESELLER, fafdf38b16bf6b2b
    lijit.com, 273644, RESELLER, fafdf38b16bf6b2b
    onetag.com, 5d1628750185ace, RESELLER
    appnexus.com, 12941, RESELLER, f5ab79cb980f11d1
    nobid.io, 22501800598, RESELLER
    betweendigital.com, 44095, RESELLER
    onetag.com, 73720b89bbda622, RESELLER
    improvedigital.com, 1880, RESELLER
    prequel.tv, 513, RESELLER
    improvedigital.com, 1751, RESELLER
    appnexus.com, 12752, RESELLER, f5ab79cb980f11d1
    improvedigital.com, 1150, RESELLER
    appnexus.com, 12969, RESELLER, f5ab79cb980f11d1
    targetspot.com, 270, RESELLER, feb28ed826dcf532
    onetag.com, 7519b266468320e, RESELLER
    beachfront.com, 7300, RESELLER
    appnexus.com, 13293, RESELLER, f5ab79cb980f11d1
    pubmatic.com, 161372, RESELLER, 5d62403b186f2ace
    webeyemob.com, 70100, RESELLER
    openx.com, 540679900, RESELLER, 6a698e2ec38604c6
    adcolony.com, 801e49d1be83b5f9, RESELLER, 1ad675c9de6b5176
    pubmatic.com, 158060, RESELLER, 5d62403b186f2ace
    meitu.com, 663, RESELLER
    acd.op.hicloud.com, PUB_HW_1003, RESELLER
    bigo.sg, 128, RESELLER
    ignitemediatech.com, pub_61118, RESELLER
    admixer.net, 2f833c20-7378-4b86-9b73-a2b56263d4d4, RESELLER
    algorix.co, 60204, RESELLER, 5b394c12fea27a1d
    loopme.com, 11295, RESELLER, 6c8d5f95897a5a3b
    openx.com, 542281387, RESELLER, 6a698e2ec38604c6
    pubmatic.com, 160849, RESELLER, 5d62403b186f2ace
    pubnative.net, 1007334, RESELLER, d641df8625486a7b
    pubnative.net, 1007615, RESELLER, d641df8625486a7b
    silvermob.com, 464, RESELLER
    smaato.com, 1100044156, RESELLER, 07bcf65f187117b4
    spotxchange.com, 307720, RESELLER, 7842df1d2fe2db34
    webeyemob.com, 70098, RESELLER
    admixer.co.kr, 1317, RESELLER
    onetag.com, 66cff8e37d871be, RESELLER
    indexexchange.com, 192806, RESELLER, 50b1c356f2c5c8fc
    indexexchange.com, 182257, RESELLER
    rhythmone.com, 123476257, RESELLER, a670c89d4a324e47
    pubmatic.com, 158565, RESELLER, 5d62403b186f2ace
    opera.com, pub4007008646336, RESELLER, 55a0c5fd61378de3
    triplelift.com, 10522, RESELLER, 6c33edb13117fd86
    smartadserver.com, 4052, RESELLER
    synacor.com, 82460, RESELLER, e108f11b2cdf7d5b
    aol.com, 58935, RESELLER, e1a5b5b6e3255540
    yahoo.com, 58935, RESELLER, e1a5b5b6e3255540
    loopme.com, 11414, RESELLER, 6c8d5f95897a5a3b
    rubiconproject.com, 20744, RESELLER, 0bfd66d529a55807
    engagebdr.com, 10181, RESELLER
    smartyads.com, 100034, RESELLER, fd2bde0ff2e62c5d
    eskimi.com, eas-2020000005, RESELLER
    pubmatic.com, 158154, RESELLER, 5d62403b186f2ace
    admanmedia.com, 594, RESELLER
    admixer.net, eab09ea6-bcf7-4eff-965d-b4f43962c73c, RESELLER
    algorix.co, 54510, RESELLER, 5b394c12fea27a1d
    algorix.co, 60095, RESELLER, 5b394c12fea27a1d
    algorix.co, 60097, RESELLER, 5b394c12fea27a1d
    algorix.co, 60084, RESELLER, 5b394c12fea27a1d
    algorix.co, 60206, RESELLER, 5b394c12fea27a1d
    indexexchange.com, 193747, RESELLER, 50b1c356f2c5c8fc
    kubient.com, 5e2cb089c866, RESELLER, 4f12311e6ed900a3
    ucfunnel.com, par-D2346AAB7ABD36B4CDD7BBD264BA92E2, RESELLER
    aralego.com, par-D2346AAB7ABD36B4CDD7BBD264BA92E2, RESELLER
    smaato.com, 1100042823, RESELLER, 07bcf65f187117b4
    smaato.com, 1100004890, RESELLER, 07bcf65f187117b4
    tpmn.io, 415, RESELLER
    engagebdr.com, 10423, RESELLER
    peak226.com, 12900, RESELLER
    peak226.com, 12901, RESELLER
    pubmatic.com, 160113, RESELLER, 5d62403b186f2ace
    video.unrulymedia.com, 3704396951, RESELLER
    spotxchange.com, 234183, RESELLER, 7842df1d2fe2db34
    spotx.tv, 234183, RESELLER, 7842df1d2fe2db34
    indexexchange.com, 191332, RESELLER, 50b1c356f2c5c8fc
    advertising.com, 28246, RESELLER
    sonobi.com, eaec54c63f, RESELLER, d1a215d9eb5aee9e
    ucfunnel.com, par-6272997D742EBBBB1E4B8B6D9EA8D3B7, RESELLER
    olaex.biz, 100039, RESELLER
    smartadserver.com, 3817, RESELLER
    ucfunnel.com, par-E2B2BB3E89BE87A2F774B768BEED62A2, RESELLER
    rubiconproject.com, 20050, RESELLER, 0bfd66d529a55807
    openx.com, 540838151, RESELLER, 6a698e2ec38604c6
    inmobi.com, 22e5354e453f49348325184e25464adb, RESELLER, 83e75a7ae333ca9d
    admixer.net, a80e4ca1-f52f-4aa5-98e1-010c8d08251b, RESELLER
    contextweb.com, 562146, RESELLER, 89ff185a4c4e857c
    smartyads.com, 897, RESELLER, fd2bde0ff2e62c5d
    pubnative.net, 1007262, RESELLER, d641df8625486a7b
    peak226.com, 12400, RESELLER
    epom.com, 9228dad8-6fd8-4e11-8885-f5a02dc42604, RESELLER
    gothamads.com, 399, RESELLER, d9c86e5dec870222
    onetag.com, 5a02ff98ba6be67, RESELLER
    admanmedia.com, 621, RESELLER
    max-mobi.com, 11456, RESELLER, 6c4d5f85197a5b3c
    pubmatic.com, 157800, RESELLER, 5d62403b186f2ace
    pubmatic.com, 158118, RESELLER, 5d62403b186f2ace
    rubiconproject.com, 18364, RESELLER, 0bfd66d529a55807
    pubnative.net, 1007215, RESELLER, d641df8625486a7b
    pubmatic.com, 155975, RESELLER, 5d62403b186f2ace
    openx.com, 537152826, RESELLER, 6a698e2ec38604c6
    openx.com, 539472296, RESELLER, 6a698e2ec38604c6
    appnexus.com, 8178, RESELLER, f5ab79cb980f11d1
    rubiconproject.com, 17328, RESELLER, 0bfd66d529a55807
    adcolony.com, 1efc6603710003ea, RESELLER, 1ad675c9de6b5176
    contextweb.com, 561884, RESELLER, 89ff185a4c4e857c
    rhythmone.com, 4173858586, RESELLER, a670c89d4a324e47
    improvedigital.com, 1366, RESELLER
    emxdgt.com, 1324, RESELLER, 1e1d41537f7cad7f
    engagebdr.com, 10252, RESELLER
    bidmachine.io, 55, RESELLER
    bidmachine.io, 59, RESELLER
    inmobi.com, f3924290136e4129a5c082ff982c3a58, RESELLER, 83e75a7ae333ca9d
    verve.com, 15290, RESELLER, 0c8f5958fc2d6270
    inmobi.com, c1e6d3502da64ebaa3ad0e4a4be15f11, RESELLER, 83e75a7ae333ca9d
    pubmatic.com, 156517, RESELLER, 5d62403b186f2ace
    openx.com, 540543195, RESELLER, 6a698e2ec38604c6
    contextweb.com, 561849, RESELLER, 89ff185a4c4e857c
    admixer.net, 8e380da6-31ba-488c-939c-290c48d577e4, RESELLER
    yieldnexus.com, 96204, RESELLER
    onetag.com, 59aa7be4921bac8, RESELLER
    pokkt.com, 6953, RESELLER, c45702d9311e25fd
    spotxchange.com, 117872, RESELLER, 7842df1d2fe2db34
    spotx.tv, 117872, RESELLER, 7842df1d2fe2db34
    aralego.com, par-E2B2BB3E89BE87A2F774B768BEED62A2, RESELLER
    pubmatic.com, 157941, RESELLER, 5d62403b186f2ace
    loopme.com, 11119, RESELLER, 6c8d5f95897a5a3b
    chocolateplatform.com, 14637, RESELLER, 49a66ce31a704197
    bidmachine.io, 35, RESELLER
    adcolony.com, c490f6e7399a25d6, RESELLER, 1ad675c9de6b5176
    appnexus.com, 10824, RESELLER
    appnexus.com, 9569, RESELLER
    chartboost.com, 5da62a1035b91e0aff190bf7, RESELLER
    districtm.io, 101649, RESELLER, 3fd707be9c4527c3
    google.com, pub-9685734445476814, RESELLER, f08c47fec0942fa0
    groundtruth.com, 107, RESELLER, 81cbf0a75a5e0e9a
    pubmatic.com, 156435, RESELLER, 5d62403b186f2ace
    pubmatic.com, 158111, RESELLER, 5d62403b186f2ace
    pubmatic.com, 158112, RESELLER, 5d62403b186f2ace
    pubmatic.com, 92509, RESELLER, 5d62403b186f2ace
    rubiconproject.com, 13856, RESELLER, 0bfd66d529a55807
    smartadserver.com, 1692, RESELLER
    indexexchange.com, 198417, RESELLER, 50b1c356f2c5c8fc
    themediagrid.com, NG9STC, RESELLER, 35d5010d7789b49d
    video.unrulymedia.com, 3383599585, RESELLER
    rhythmone.com, 3383599585, RESELLER, a670c89d4a324e47
    rubiconproject.com, 24526, RESELLER, 0bfd66d529a55807
    yandex.com, 97539269, RESELLER
    inmobi.com, 062029933580429f9920bad476d8d70a, RESELLER, 83e75a7ae333ca9d
    inmobi.com, d708160556074572b05ed3cf112bb66f, RESELLER, 83e75a7ae333ca9d
    inmobi.com, 6cc71dd159864641a03ce0c8792d801f, RESELLER, 83e75a7ae333ca9d
    rubiconproject.com, 24362, RESELLER, 0bfd66d529a55807
    admanmedia.com, 990, RESELLER
    pubmatic.com, 161136, RESELLER, 5d62403b186f2ace
    opera.com, pub5865193350528, RESELLER, 55a0c5fd61378de3
    meitu.com, 699, RESELLER
    outbrain.com, 0005bfbc60db1570e63954937b4bec06de, RESELLER
    appnexus.com, 7597, RESELLER, f5ab79cb980f11d1
    tremorhub.com, q017o-78mlk, RESELLER, 1a4e959a1b50034a
    spotxchange.com, 225721, RESELLER
    freewheel.tv, 741650, RESELLER
    rubiconproject.com, 17130, RESELLER, 0bfd66d529a55807
    lkqd.net, 450, RESELLER, 59c49fa9598a0117
    openx.com, 540393169, RESELLER, 6a698e2ec38604c6
    spotx.tv, 238936, RESELLER, 7842df1d2fe2db34
    spotxchange.com, 238936, RESELLER, 7842df1d2fe2db34
    rubiconproject.com, 19668, RESELLER, 0bfd66d529a55807
    indexexchange.com, 190856, RESELLER, 50b1c356f2c5c8fc
    pubmatic.com, 158615, RESELLER, 5d62403b186f2ace
    vidazoo.com, 1773068026, RESELLER, b6ada874b4d7d0b2
    indexexchange.com, 193091, RESELLER, 50b1c356f2c5c8fc
    pubmatic.com, 160065, RESELLER, 5d62403b186f2ace
    improvedigital.com, 1863, RESELLER
    freewheel.tv, 1220655, RESELLER
    yahoo.com, 59052, RESELLER
    risecodes.com, 6022acddc8b2f90001767980, RESELLER
    gitberry.com, 325100011, RESELLER
    gitberry.com, 325100012, RESELLER
    pubmatic.com, 161601, RESELLER, 5d62403b186f2ace
    contextweb.com, 562724, RESELLER, 89ff185a4c4e857c
    improvedigital.com, 1361, RESELLER
    triplelift.com, 12552, RESELLER, 6c33edb13117fd86
    lunamedia.io, e6052bf0717e11eda1b0b561fab79f1a, RESELLER, 524ecb396915caaf
    contextweb.com, 562329, RESELLER, 89ff185a4c4e857c
    pubmatic.com, 160493, RESELLER, 5d62403b186f2ace
    video.unrulymedia.com, 4631344382657206988, RESELLER
    inmobi.com, ddb41d8a9f434a918d05a0fc9999d9f9, RESELLER, 83e75a7ae333ca9d
    yahoo.com, 59627, RESELLER
    xandr.com, 12745, RESELLER, f5ab79cb980f11d1
    conversantmedia.com, 100322, RESELLER, 03113cd04947736d
    inmobi.com, 389d602d8990410183b192230ca7afa9, RESELLER, 83e75a7ae333ca9d
    smartadserver.com, 4539, RESELLER, 060d053dcf45cbf3
    contextweb.com, 562878, RESELLER, 89ff185a4c4e857c
    xandr.com, 13799, RESELLER
    triplelift.com, 12158, RESELLER, 6c33edb13117fd86
    freewheel.tv, 1137745, RESELLER
    freewheel.tv, 1138513, RESELLER
    opera.com, pub7275292332480, RESELLER, 55a0c5fd61378de3
    improvedigital.com, 2110, RESELLER
    gitberry.com, 42knb, RESELLER
    media.net, 8CUN37DCC, RESELLER
    sharethrough.com, 6qlnf8SY, RESELLER, d53b998a7bd4ecd2
    onairglobal.com, 4357627, RESELLER
    prequel.tv, 515, RESELLER
    onetag.com, 7ba1175dffb3110, RESELLER
    openx.com, 540298543, RESELLER, 6a698e2ec38604c6
    conversantmedia.com, 100246, RESELLER, 03113cd04947736d
    improvedigital.com, 1532, RESELLER
    mars.media, 1010443, RESELLER, 8624339f102fb076
    thebrave.io, 1234585, RESELLER, c25b2154543746ac
    ssp.e-volution.ai, AJxF6R118a9M6CaTvK, RESELLER
    loopme.com, 11367, RESELLER, 6c8d5f95897a5a3b
    inmobi.com, 30f3830cfef249a3ad46ee1a0bba7af3, RESELLER, 83e75a7ae333ca9d
    myfeature.tv, UvnXhUMZHRvKfRk52QZy, RESELLER
    video.unrulymedia.com, 1876793108340587286, RESELLER
    inmobi.com, cac51fe616224c3c8f858f34b3267dd7, RESELLER, 83e75a7ae333ca9d
    omnifytv.com, 05096ffe925c11eda1eb0242ac120002, RESELLER
    inmobi.com, 662fb54f5d15471a80b4ddaa467e84b0, RESELLER, 83e75a7ae333ca9d
    bematterfull.com, 562499, RESELLER
    triplelift.com, 13423, RESELLER, 6c33edb13117fd86
    bigo.sg, 109, RESELLER
    pubmatic.com, 162588, RESELLER, 5d62403b186f2ace
    loopme.com, 11463, RESELLER, 6c8d5f95897a5a3b
    ignitemediatech.com, pub_61351, RESELLER
    outbrain.com, 009003959a5821f830a09d0bc562b43c62, RESELLER
    pangleglobal.com, 45779, RESELLER
    yandex.com, 63904735, RESELLER
    yieldmo.com, 3036313774475649025, RESELLER
    eskimi.com, 2020000016, RESELLER
    smartadserver.com, 4573, RESELLER, 060d053dcf45cbf3
    rubiconproject.com, 16114, RESELLER, 0bfd66d529a55807
    openx.com, 537149888, RESELLER, 6a698e2ec38604c6
    appnexus.com, 3703, RESELLER, f5ab79cb980f11d1
    loopme.com, 5679, RESELLER, 6c8d5f95897a5a3b
    xad.com, 958, RESELLER, 81cbf0a75a5e0e9a
    video.unrulymedia.com, 2564526802, RESELLER
    smaato.com, 1100044045, RESELLER, 07bcf65f187117b4
    pubnative.net, 1006576, RESELLER, d641df8625486a7b
    axonix.com, 57264, RESELLER
    sharethrough.com, OAW69Fon, RESELLER, d53b998a7bd4ecd2
    contextweb.com, 562791, RESELLER, 89ff185a4c4e857c
    consumable.com, 2001470, RESELLER, aefcd3d2f45b5070
    contextweb.com, 562954, RESELLER, 89ff185a4c4e857c
    pubmatic.com, 162270, RESELLER, 5d62403b186f2ace
    videoheroes.tv, 212503, RESELLER, 064bc410192443d8
    Se7en.es, 212503, RESELLER, 064bc410192443d8
    yeahmobi.com, 5135284, RESELLER
    lunamedia.io, 2fb901cd79f4453b90b68bee79da71b5, RESELLER, 524ecb396915caaf
    gothamads.com, 1236, RESELLER, d9c86e5dec870222
    growintech.co, 101696, RESELLER
    e-planning.net, 9522026ef023606d, RESELLER, c1ba615865ed87b2
    conversantmedia.com, 100519, RESELLER, 03113cd04947736d
    admanmedia.com, 2050, RESELLER
    appnexus.com, 4052, RESELLER
    contextweb.com, 561998, RESELLER, 89ff185a4c4e857c
    lijit.com, 411121, RESELLER, fafdf38b16bf6b2b
    openx.com, 540031703, RESELLER, 6a698e2ec38604c6
    pubmatic.com, 158100, RESELLER, 5d62403b186f2ace
    rubiconproject.com, 23644, RESELLER, 0bfd66d529a55807
    yahoo.com, 55771, RESELLER, e1a5b5b6e3255540
    admixer.net, f6790c61-b9ce-49b0-b809-059710d60dfd, RESELLER
    imds.tv, 82578, RESELLER, ae6c32151e71f19d
    inmobi.com, 625b6b37c3a340938a8ff02bc83b5a04, RESELLER, 83e75a7ae333ca9d
    sharethrough.com, 3blZyYks, RESELLER, d53b998a7bd4ecd2
    pubmatic.com, 160492, RESELLER, 5d62403b186f2ace
    freewheel.tv, 1585279, RESELLER
    videoheroes.tv, 212581, RESELLER, 064bc410192443d8
    rubiconproject.com, 25060, RESELLER, 0bfd66d529a55807
    sharethrough.com, K5WDGAsP, RESELLER, d53b998a7bd4ecd2
    sonobi.com, c58cf86903, RESELLER, d1a215d9eb5aee9e
    improvedigital.com, 1805, RESELLER
    contextweb.com, 562794, RESELLER, 89ff185a4c4e857c
    criteo.com, B-064322, RESELLER, 9fac4a4a87c2a44f
    video.unrulymedia.com, 170071695, RESELLER
    startapp.com, wbe, RESELLER
    start.io, wbe, RESELLER
    admixer.net, 70590d13-5632-41d5-8a78-76bc239c528c, RESELLER
    bidence.com, 4a87393d59a18d4bbfb5326c5d98051a, RESELLER
    pubmatic.com, 163420, RESELLER, 5d62403b186f2ace
    smartadserver.com, 4140, RESELLER
    triplelift.com, 11656, RESELLER, 6c33edb13117fd86
    vidoomy.com, 4930225, RESELLER
    smartadserver.com, 4457, RESELLER, 060d053dcf45cbf3
    loopme.com, 11605, RESELLER, 6c8d5f95897a5a3b
    Xapads.com, 155860, RESELLER
    betweendigital.com, 45117, RESELLER
    pubmatic.com, 162968, RESELLER, 5d62403b186f2ace
    brightcom.com, 29559, RESELLER
    pubnative.net, 1008379, RESELLER, d641df8625486a7b
    criteo.com, B-070302, RESELLER, 9fac4a4a87c2a44f
    thunder-monetize.com, 4588767099, RESELLER
    opera.com, pub9598692093632, RESELLER, 55a0c5fd61378de3
    smartadserver.com, 4762, RESELLER, 060d053dcf45cbf3
    appnexus.com, 15426, RESELLER, f5ab79cb980f11d1
    xandr.com, 15426, RESELLER, f5ab79cb980f11d1
    smaato.com, 1100055341, RESELLER, 07bcf65f187117b4
    rubiconproject.com, 24600, RESELLER, 0bfd66d529a55807
    sharethrough.com, iBAzay96, RESELLER, d53b998a7bd4ecd2
    pubmatic.com, 156177, RESELLER, 5d62403b186f2ace
    pubmatic.com, 156425, RESELLER, 5d62403b186f2ace
    inmobi.com, 55049d2e109d4ac1820ca1432dda4e13, RESELLER, 83e75a7ae333ca9d
    inmobi.com, 23800006c42e4a739966c05ca0ac4854, RESELLER, 83e75a7ae333ca9d
    openx.com, 559792583, RESELLER, 6a698e2ec38604c6
    loopme.com, 11694, RESELLER, 6c8d5f95897a5a3b
    conversantmedia.com, 100611, RESELLER, 03113cd04947736d
    triplelift.com, 11000, RESELLER, 6c33edb13117fd86
    algorix.co, 604550, RESELLER, 5b394c12fea27a1d
    freewheel.tv, 1601610, RESELLER
    freewheel.tv, 1601613, RESELLER
    inmobi.com, 0c2fa8130b884712913cc0bfc84e1c31, RESELLER, 83e75a7ae333ca9d
    video.unrulymedia.com, 689559416, RESELLER
    unrulymedia.com, 689559416, RESELLER
    equativ.com, 4762, RESELLER, 060d053dcf45cbf3
    thunder-monetize.com, 4818959194, RESELLER
    thunder-monetize.com, 6343837988, RESELLER
    thunder-monetize.com, 1723306631, RESELLER
    rubiconproject.com, 26064, RESELLER, 0bfd66d529a55807
    atasdigital.com, 504422, RESELLER
    inmobi.com, 30eb9116886a497db0c502e8cf0aa4d3, RESELLER, 83e75a7ae333ca9d
    rubiconproject.com, 26086, RESELLER, 0bfd66d529a55807
    lijit.com, 473789, RESELLER, fafdf38b16bf6b2b
    pubmatic.com, 164018, RESELLER, 5d62403b186f2ace
    pubmatic.com, 164019, RESELLER, 5d62403b186f2ace
    appnerve.com, 187287, RESELLER
    smartadserver.com, 4625, RESELLER, 060d053dcf45cbf3
    lijit.com, 400766, RESELLER, fafdf38b16bf6b2b
    sovrn.com, 400766, RESELLER, fafdf38b16bf6b2b
    pubnative.net, 1007303, RESELLER, d641df8625486a7b
    pubnative.net, 1007311, RESELLER, d641df8625486a7b
    smartadserver.com, 4568, RESELLER, 060d053dcf45cbf3
    rubiconproject.com, 22134, RESELLER, 0bfd66d529a55807
    bigo.sg, 1011110, RESELLER
    inmobi.com, edd282ac8f29464792bf2b7f3df2f9df, RESELLER, 83e75a7ae333ca9d
    smartadserver.com, 4610, RESELLER, 060d053dcf45cbf3
    ignitemediatech.com, pub_1001, RESELLER
    lijit.com, 367217, RESELLER, fafdf38b16bf6b2b
    thunder-monetize.com, 2297825074, RESELLER
    thunder-monetize.com, 3692155372, RESELLER
    thunder-monetize.com, 3905806597, RESELLER
    themediagrid.com, 7MYRZ3, RESELLER, 35d5010d7789b49d
    criteo.com, B-072581, RESELLER, 9fac4a4a87c2a44f
    ignitemediatech.com, pub_11112, RESELLER
    lijit.com, 483304, RESELLER, fafdf38b16bf6b2b
    caerulus.io, 3320-39849, RESELLER
    adform.com , 2742 , RESELLER
    lacunads.com, LC136f79e999464edc, RESELLER
    smartyads.com,300045, RESELLER, fd2bde0ff2e62c5d
    video.unrulymedia.com, 645663965, RESELLER
    advertising.com, 23089, RESELLER
    advertising.com, 28605, RESELLER
    aniview.com, 603f65a2e291680ef30af9c7, RESELLER, 78b21b97965ec3f8
    appnexus.com, 12637, RESELLER, f5ab79cb980f11d1
    appnexus.com, 9382, RESELLER, f5ab79cb980f11d1
    google.com, pub-6346866704322274, RESELLER, f08c47fec0942fa0
    google.com, pub-3565385483761681, RESELLER, f08c47fec0942fa0
    google.com, pub-5717092533913515, RESELLER, f08c47fec0942fa0
    pubmatic.com, 159277, RESELLER, 5d62403b186f2ace
    pubmatic.com, 161335, RESELLER, 5d62403b186f2ace
    pubmatic.com, 160993, RESELLER, 5d62403b186f2ace
    rubiconproject.com, 13918, RESELLER, 0bfd66d529a55807
    synacor.com, 82171, RESELLER, e108f11b2cdf7d5b
    yahoo.com, 59244, RESELLER
    Improvedigital.com, 1699, RESELLER
    openx.com, 543878511, RESELLER, 6a698e2ec38604c6
    bidease.com, bidease_seller_34, RESELLER
    tredio.io, 357a6fdf7642bf815a88822c447d9dc433546, RESELLER
    video.unrulymedia.com, 819070080, RESELLER
    bold-win.com, 807, RESELLER, 71746737d0bab951
    flat-ads.com,2196901,RESELLER
    google.com, pub-3990748024667386, RESELLER, f08c47fec0942fa0
    criteo.com, B-071797, RESELLER, 9fac4a4a87c2a44f
    themediagrid.com, SH96GF, RESELLER, 35d5010d7789b49d
    lijit.com, 500520 , RESELLER, fafdf38b16bf6b2b
    video.unrulymedia.com, 949394431, RESELLER
    appaspire.com, 97517, RESELLER
    contextweb.com, 563195, RESELLER, 89ff185a4c4e857c
    smaato.com, 1100056110, RESELLER, 07bcf65f187117b4
    ylhglobal.com, 1930587443, RESELLER
    video.unrulymedia.com, 827123801, RESELLER
    toponad.com,1661f80179644e,DIRECT,1d49fe424a1a456d
    pubmatic.com, 160846, RESELLER, 5d62403b186f2ace
    webeyemob.com, 70098, DIRECT
    conversantmedia.com, 100358, RESELLER, 03113cd04947736d
    rubiconproject.com, 25978, RESELLER, 0bfd66d529a55807
    onetag.com, 8668c16092f01e8, RESELLER
    e-planning.net,79686787743ddfc5,RESELLER,c1ba615865ed87b2
    ads.vk.com, 18644219, RESELLER
    playwire.com,1025119,RESELLER
    inventorypartnerdomain=thunder-monetize.com
    thunder-monetize.com, 1125520627, RESELLER
    inventorypartnerdomain=rhebus.works
    rhythmone.com, 689559416, RESELLER, a670c89d4a324e47
    lijit.com, 411121, RESELLER, fafdf38b16bf6b2b #SOVRN
    video.unrulymedia.com, 905340553, RESELLER
    conversantmedia.com, 100569, RESELLER, 03113cd04947736d
    taurusx.com, 80609, RESELLER
    webeyemob.com, 80609, RESELLER
    caerulus.io, 3320-44758, RESELLER
    rubiconproject.com, 24170, RESELLER, 0bfd66d529a55807
    zetaglobal.net, 681, RESELLER
    improvedigital.com, 2451, RESELLER
    sharethrough.com, UbYLkrys, RESELLER, d53b998a7bd4ecd2
    sharethrough.com, g4qXILSV, RESELLER, d53b998a7bd4ecd2
    thunder-monetize.com, 5016320847, RESELLER
    thunder-monetize.com, 8227779878, RESELLER
    thunder-monetize.com, 4116628010, RESELLER
    thunder-monetize.com, 2626756011, RESELLER
    thunder-monetize.com, 6394460651, RESELLER
    thunder-monetize.com, 9214052224, RESELLER
    thunder-monetize.com, 5102475972, RESELLER
    thunder-monetize.com, 9347471687, RESELLER
    thunder-monetize.com, 3879160930, RESELLER
    thunder-monetize.com, 7169228962, RESELLER
    thunder-monetize.com, 9326167394, RESELLER
    thunder-monetize.com, 3199604937, RESELLER
    thunder-monetize.com, 6124986497, RESELLER
    connatix.com, 1713048496211545, RESELLER, 2af98acdee0e81ed
    themediagrid.com, R28I9J, RESELLER, 35d5010d7789b49d
    rhebus.works, 9347471687, RESELLER
    thunder-monetize.com, 6329293809, RESELLER
    thunder-monetize.com, 3018968091, RESELLER
    thunder-monetize.com, 1858090629, RESELLER
    thunder-monetize.com, 5462031088, RESELLER
    thunder-monetize.com, 4586080114, RESELLER
    thunder-monetize.com, 8534885299, RESELLER
    thunder-monetize.com, 7373737053, RESELLER
    thunder-monetize.com, 9630185716, RESELLER
    thunder-monetize.com, 1257832012, RESELLER
    thunder-monetize.com, 3081642139, RESELLER
    thunder-monetize.com, 3797245630, RESELLER
    thunder-monetize.com, 5714748458, RESELLER
    thunder-monetize.com, 6268002838, RESELLER
    thunder-monetize.com, 7213903648, RESELLER
    thunder-monetize.com, 6747258203, RESELLER
    thunder-monetize.com, 2158729563, RESELLER
    thunder-monetize.com, 8888198173, RESELLER
    thunder-monetize.com, 9653037459, RESELLER
    thunder-monetize.com, 4306738719, RESELLER
    thunder-monetize.com, 8432987942, RESELLER
    thunder-monetize.com, 1947298216, RESELLER
    thunder-monetize.com, 8764970206, RESELLER
    thunder-monetize.com, 1933963830, RESELLER
    thunder-monetize.com, 9153035698, RESELLER
    thunder-monetize.com, 5803687805, RESELLER
    video.unrulymedia.com, 906352066, RESELLER
    rubiconproject.com, 15268, RESELLER, 0bfd66d529a55807
    rubiconproject.com, 15268, DIRECT, 0bfd66d529a55807
    appnexus.com, 6849, RESELLER
    pubmatic.com, 159382, RESELLER
    pubmatic.com, 161058, RESELLER, 5d62403b186f2ace
    smaato.com, 1100047589, RESELLER, 07bcf65f187117b4
    inmobi.com, ba5fd3fb82c5412989b23c3eec71baf7,RESELLER,83e75a7ae333ca9d
    loopme.com, 9718, RESELLER, 6c8d5f95897a5a3b
    yeahmobi.com, 114490, RESELLER
    zmaticoo.com, 114490, RESELLER
    adingo.jp, 31054, RESELLER
    singularads.com, 548745, RESELLER
    yieldmo.com, 3591550211052216798, RESELLER, 6a92c77cfc3d2258
    App-stock.com ,558223, Reseller, ed8c126ea5971415
    contextweb.com, 563321, RESELLER, 89ff185a4c4e857c
    pubmatic.com, 165329, RESELLER, 5d62403b186f2ace
    smaato.com, 1100057628, DIRECT, 07bcf65f187117b4
    triplelift.com, 12240,RESELLER, 6c33edb13117fd86
    rubiconproject.com, 25064, RESELLER, 0bfd66d529a55807
    appnexus.com,15980,RESELLER,f5ab79cb980f11d1
    sonobi.com, d2b13d1c36, RESELLER, d1a215d9eb5aee9e
    smartadserver.com , 5026, RESELLER, 060d053dcf45cbf3
    equativ.com, 5026, RESELLER, 060d053dcf45cbf3
    onetag.com, 8dd8de7f341d57e, RESELLER
    pubmatic.com, 164119, RESELLER, 5d62403b186f2ace
    contextweb.com,563539,RESELLER,89ff185a4c4e857c
    onetag.com, 8dd92b4434e684b, RESELLER
    e-planning.net,1bf7b5d803f178c4,RESELLER,c1ba615865ed87b2
    rubiconproject.com, 26958, RESELLER, 0bfd66d529a55807
    onetag.com, 8df76ed1d09d55e, RESELLER
    consumable.com, 2001585, RESELLER, aefcd3d2f45b5070
    tritondigital.com, 106423, RESELLER, 19b4454d0b87b58b
    adswizz.com, 561, RESELLER
    zetaglobal.net, 815, RESELLER
    sabio.us,100092,reseller,96ed93aaa9795702
    freewheel.tv,1606620,reseller
    freewheel.tv,1606633,reseller
    rubiconproject.com,17608,RESELLER,0bfd66d529a55807
    lijit.com,465542,RESELLER,fafdf38b16bf6b2b
    video.unrulymedia.com,123476257,RESELLER
    onetag.com,82e44d118b79600,DIRECT
    themediagrid.com, A6CWLO, RESELLER, 35d5010d7789b49d
    admatic.com.tr, adm-pub-3033566192, DIRECT, uufps1dh5stc6euk
    rubiconproject.com, 25100, RESELLER, 0bfd66d529a55807
    contextweb.com,562911,RESELLER,89ff185a4c4e857c
    improvedigital.com, 2276, RESELLER
    pubmatic.com,165340,RESELLER,5d62403b186f2ace
    yeahmobi.com,104752, RESELLER
    admixer.net, f6790c61-b9ce-49b0-b809-059710d60dfd, DIRECT
    inmobi.com, 61d733c3779d43e590c51c8bc078e10c, RESELLER, 83e75a7ae333ca9d
    e-planning.net,ec771b05828a67fa,RESELLER,c1ba615865ed87b2
    richaudience.com, 9komJKwMhZ, RESELLER
    opera.com,pub11079554715136,DIRECT,55a0c5fd61378de3
    lijit.com, 530498, RESELLER, fafdf38b16bf6b2b
    showheroes.com, 6031, RESELLER
    smartclip.net, 27985, RESELLER
    lijit.com, 380632, RESELLER, fafdf38b16bf6b2b
    krushmedia.com, AJxF6R192a9M6CaTvK, DIRECT
    conversantmedia.com, 100863, RESELLER, 03113cd04947736d
    appaspire.com, 97515, RESELLER
    rubiconproject.com, 26694, RESELLER, 0bfd66d529a55807
    pubmatic.com, 165702, RESELLER, 5d62403b186f2ace
    pubmatic.com, 165701, RESELLER, 5d62403b186f2ace
    inmobi.com, 6cb2de20b77b4011911f2df29e4674a0, RESELLER, 83e75a7ae333ca9d
    contextweb.com, 563462, RESELLER, 89ff185a4c4e857c
    atasdigital.com, 506154, RESELLER
    uis.mobfox.com, 2206, RESELLER, 5529a3d1f59865be
    singularads.com, 445896, RESELLER
    triplelift.com, 13792, RESELLER, 6c33edb13117fd86
    lijit.com, 528054, RESELLER, fafdf38b16bf6b2b 
    rubiconproject.com, 17960, RESELLER, 0bfd66d529a55807
    pubmatic.com, 137711, RESELLER, 5d62403b186f2ace
    smaato.com, 1100056344, RESELLER, 07bcf65f187117b4
    openx.com, 538959099, RESELLER, 6a698e2ec38604c6
    smartadserver.com , 4926, RESELLER, 060d053dcf45cbf3
    contextweb.com,563601,RESELLER,89ff185a4c4e857c
    pubnative.net, 1009988, RESELLER, d641df8625486a7b
    pubmatic.com, 161368, RESELLER, 5d62403b186f2ace
    themediagrid.com, NG9STC, DIRECT, 35d5010d7789b49d
    improvedigital.com, 2505, RESELLER
    sovrn.com, 500520, RESELLER, fafdf38b16bf6b2b
    zetaglobal.com, 815, RESELLER
    ssp.zetaglobal.net, 815, RESELLER
    ucfunnel.com, par-E2B3A33EAA4286BEF7ADD9AA28A922D2, RESELLER
    aralego.com, par-E2B3A33EAA4286BEF7ADD9AA28A922D2, RESELLER
    adiiix.com, db716a4c, RESELLER
    rubiconproject.com, 25482, RESELLER, 0bfd66d529a55807
    krushmedia.com, AJxF6R192a9M6CaTvK, RESELLER
    mgid.com, 885240, RESELLER, d4c29acad76ce94f
    rubiconproject.com, 26552, RESELLER
    admixer.co.kr, 1289, RESELLER
    zmaticoo.com, 114490, DIRECT
    contextweb.com,563521,RESELLER,89ff185a4c4e857c
    rubiconproject.com, 15044, RESELLER, 0bfd66d529a55807
    admanmedia.com, 2248, RESELLER
    improvedigital.com, 2508, RESELLER
    improvedigital.com, 2509, RESELLER
    pubnative.net, 1010067, RESELLER, d641df8625486a7b
    pubnative.net, 1010068, RESELLER, d641df8625486a7b
    pubnative.net, 1010069, RESELLER, d641df8625486a7b
    pubnative.net, 1010070, RESELLER, d641df8625486a7b
    pubnative.net, 1010071, RESELLER, d641df8625486a7b
    pubnative.net, 1010072, RESELLER, d641df8625486a7b
    pubnative.net, 1010073, RESELLER, d641df8625486a7b
    connekt.ai, 2852512, RESELLER
    adyoulike.com, e48120b055ac0abcc41c0093bf3fe02e, RESELLER
    lijit.com, 543478, RESELLER, fafdf38b16bf6b2b
    bidmachine.io,896,RESELLER
    video.unrulymedia.com, 557688749, RESELLER
    openx.com,540396775,RESELLER,6a698e2ec38604c6
    pubmatic.com,157800,RESELLER,5d62403b186f2ace
    rubiconproject.com,18364,RESELLER,0bfd66d529a55807
    xandr.com,13293,RESELLER,f5ab79cb980f11d1
    inmobi.com, 95e2b3b059d64b2c97e4cf8dc2b15796, RESELLER, 83e75a7ae333ca9d
    appsheep.com, 27dee787560b471cafde1e55d87bc75b, RESELLER
    appsheep.com, 225239742b9b4299988a6ac86d11f752, RESELLER
    appsheep.com, c34528d5387e44158367ab905d2df86a, RESELLER
    appsheep.com, 73bdcb5d7d51463fa655b87fa6847051, RESELLER
    appsheep.com, 31fdfbfe3d0e4a6da5305ff849648024, RESELLER
    appsheep.com, ae4e0e2cebe3438aa18f7006b85b570d, RESELLER
    appsheep.com, fc7556810c904b029cd14668a5ee42e3, RESELLER
    appsheep.com, 3a930d9b024c457d966d480b3421e3dd, RESELLER
    appsheep.com, b2e975ab60a54848912ddde40b3e88d3, RESELLER
    appsheep.com, 14441a46f4f44c9185886bbfbd44bd5a, RESELLER
    appsheep.com, 934f1509303b4a19acb9e40d5166a8e8, RESELLER
    appsheep.com, c777b24110cc46d0a8e1245342c6c919, RESELLER
    lijit.com, 545922, RESELLER, fafdf38b16bf6b2b
    lijit.com, 545924, RESELLER, fafdf38b16bf6b2b
    themediagrid.com, GZC9B8, RESELLER, 35d5010d7789b49d
    criteo.com, B-078794, RESELLER, 9fac4a4a87c2a44f 
    media.net, 8CUI89K0D, RESELLER
    improvedigital.com, 2527, RESELLER
    improvedigital.com, 2528, RESELLER
    improvedigital.com, 2529, RESELLER
    improvedigital.com, 2530, RESELLER
    bidease.com, bidease_seller_46, RESELLER
    pubmatic.com, 165980, RESELLER, 5d62403b186f2ace
    openx.com, 561446248, RESELLER, 6a698e2ec38604c6
    loopme.com, 11635, RESELLER, 6c8d5f95897a5a3b
    pubnative.net,1008379, RESELLER, d641df8625486a7b
    inmobi.com, b01aa06531c543d8a5fb9982f60afb00, RESELLER, 83e75a7ae333ca9d
    appnexus.com, 1019, RESELLER, f5ab79cb980f11d1
    video.unrulymedia.com, 2444764291, RESELLER
    pubmatic.com, 156212, RESELLER, 5d62403b186f2ace
    contextweb.com, 558511, RESELLER, 89ff185a4c4e857c
    opera.com, pub10014056052800, RESELLER, 55a0c5fd61378de3
    motorik.io, 100463, RESELLER
    eskimi.com, 2020000810, RESELLER
    openx.com, 561365249, RESELLER, 6a698e2ec38604c6
    media.net, 8CU5L226O, RESELLER
    pubmatic.com, 159382, RESELLER
    smaato.com, 1100057546, DIRECT, 07bcf65f187117b4
    smaato.com, 1100004890, DIRECT, 07bcf65f187117b4
    singularads.com, 445896, RESELLER
    adform.com, 1762, RESELLER
    smartadserver.com, 3627, RESELLER, 060d053dcf45cbf3
    sharethrough.com, 4762, RESELLER, d53b998a7bd4ecd2
    sharethrough.com, 5026, RESELLER, d53b998a7bd4ecd2
    appnexus.com, 17044, RESELLER 
    xandr.com, 17044, RESELLER
    adyoulike.com, 76375ac0f5abb5a550c87326b9063b59, RESELLER
    adyoulike.com, 5e09d5fe0d58941e6adc167159cc9218, RESELLER
    sonobi.com, cddceeb06e, RESELLER, d1a215d9eb5aee9e
    media.net, 8CU76268D, RESELLER
    yieldmo.com, 3761877318507634863, RESELLER, 6a92c77cfc3d2258
    smaato.com, 1100058954, RESELLER, 07bcf65f187117b4
    openx.com, 562174952, RESELLER, 6a698e2ec38604c6 
    rubiconproject.com, 27130, RESELLER, 0bfd66d529a55807 
    pubeasy.io,110039,RESELLER 
    
    
    tapjoy.com, fc3c959922e04eca970e4246695608c6, DIRECT, 29e595b1aeb5904d
    rubiconproject.com, 12286, RESELLER, 0bfd66d529a55807
    webeyemob.com, 70096, RESELLER
    openx.com, 540679900, RESELLER, 6a698e2ec38604c6
    adcolony.com, 801e49d1be83b5f9, RESELLER, 1ad675c9de6b5176
    pubmatic.com, 158060, RESELLER, 5d62403b186f2ace
    video.unrulymedia.com, 123476257, RESELLER
    indexexchange.com, 182257, RESELLER
    appnexus.com, 6849, RESELLER
    rubiconproject.com, 15268, RESELLER
    rhythmone.com, 123476257, RESELLER, a670c89d4a324e47
    pubmatic.com, 160565, RESELLER, 5d62403b186f2ace
    pubmatic.com, 160565, DIRECT, 5d62403b186f2ace
    pangleglobal.com, 9898, DIRECT
    rubiconproject.com, 17608, RESELLER, 0bfd66d529a55807
    triplelift.com, 10522, RESELLER, 6c33edb13117fd86
    smartadserver.com, 4052, RESELLER
    synacor.com, 82460, DIRECT, e108f11b2cdf7d5b
    aol.com, 58935, RESELLER, e1a5b5b6e3255540
    yahoo.com, 58935, RESELLER, e1a5b5b6e3255540
    opera.com, pub4007008646336, RESELLER, 55a0c5fd61378de3
    opera.com, pub6584435113344, DIRECT, 55a0c5fd61378de3
    yahoo.com, 58935, RESELLER, e1a5b5b6e3255540
    triplelift.com, 10522, RESELLER, 6c33edb13117fd86
    appnexus.com, 13227, RESELLER
    video.unrulymedia.com, 123476257, RESELLER
    rubiconproject.com, 17608, RESELLER, 0bfd66d529a55807
    smartadserver.com, 4140, RESELLER
    contextweb.com, 562791, RESELLER, 89ff185a4c4e857c
    conversantmedia.com, 100269, RESELLER, 03113cd04947736d
    pubmatic.com, 162223, DIRECT, 5d62403b186f2ace
    smaato.com, 1100042823, RESELLER, 07bcf65f187117b4
    `),N())}})}return e})();var y0=()=>["/home/app-ads.txt"],pf=(()=>{class e{static \u0275fac=function(r){return new(r||e)};static \u0275cmp=et({type:e,selectors:[["app-main"]],standalone:!0,features:[nt],decls:73,vars:2,consts:[[1,"hero-section"],[1,"hero-content"],[1,"hero-title"],[1,"hero-subtitle"],[1,"cta-button",3,"routerLink"],[1,"about-section"],[1,"section-title"],[1,"about-content"],[1,"about-text"],["src","https://images.unsplash.com/photo-1497366754035-f200968a6e72?ixlib=rb-4.0.3","alt","Office Space",1,"about-image"],[1,"values-section"],[1,"values-grid"],[1,"value-card"],[1,"value-icon"],[1,"value-title"],[1,"value-description"],[1,"team-section"],[1,"team-grid"],[1,"team-card"],["src","https://images.unsplash.com/photo-1560250097-0b93528c311a?ixlib=rb-4.0.3","alt","CEO",1,"team-image"],[1,"team-info"],[1,"team-name"],[1,"team-position"],[1,"team-description"],["src","https://images.unsplash.com/photo-1573496359142-b8d87734a5a2?ixlib=rb-4.0.3","alt","CTO",1,"team-image"],["src","https://images.unsplash.com/photo-1519085360753-af0119f7cbe7?ixlib=rb-4.0.3","alt","COO",1,"team-image"]],template:function(r,o){r&1&&(T(0,"main")(1,"section",0)(2,"div",1)(3,"h1",2),F(4,"Welcome to HYPERSOL"),N(),T(5,"p",3),F(6,"HYPERSOL L\xE0 Doanh nghi\u1EC7p cung c\u1EA5p c\xE1c d\u1ECBch v\u1EE5 ph\xE1t tri\u1EC3n ph\u1EA7n m\u1EC1m v\xE0 CNTT c\xF3 tr\u1EE5 s\u1EDF ch\xEDnh t\u1EA1i H\xE0 N\u1ED9i. Ch\xFAng t\xF4i mang trong m\xECnh s\u1EF1 quy\u1EBFt t\xE2m, l\xF2ng nhi\u1EC7t huy\u1EBFt tu\u1ED5i tr\u1EBB, s\u1EF1 n\u1ED7 l\u1EF1c \u0111\u1EC3 ng\xE0y m\u1ED9t ph\xE1t tri\u1EC3n v\xE0 \u0111\xF3ng g\xF3p nhi\u1EC1u h\u01A1n cho ti\u1EBFn b\u1ED9 x\xE3 h\u1ED9i."),N(),T(7,"button",4),F(8,"Get TXT"),N()()(),T(9,"section",5)(10,"h2",6),F(11,"V\u1EC1 HYPERSOL"),N(),T(12,"div",7)(13,"div",8)(14,"p"),F(15,"Vui l\xF2ng li\xEAn h\u1EC7 v\u1EDBi ch\xFAng t\xF4i n\u1EBFu b\u1EA1n c\u1EA7n b\u1EA5t k\u1EF3 h\xECnh th\u1EE9c ph\xE1t tri\u1EC3n h\u1EC7 th\u1ED1ng / ph\u1EA7n m\u1EC1m / website n\xE0o \u0111\u1EC3 ph\u1EE5c v\u1EE5 cho c\xF4ng ty c\u1EE7a b\u1EA1n. Ch\xFAng t\xF4i s\u1EBD ti\u1EBFp t\u1EE5c l\xE0 \u0111\u1ED1i t\xE1c t\u1ED1t nh\u1EA5t cho c\xE1c doanh nghi\u1EC7p v\u1EEBa v\xE0 nh\u1ECF \u0111ang trong giai \u0111o\u1EA1n t\u0103ng tr\u01B0\u1EDFng v\xE0 c\xE1c nh\xE0 cung c\u1EA5p CNTT \u0111ang ph\xE1t tri\u1EC3n.."),N()(),Oe(16,"img",9),N()(),T(17,"section",10)(18,"h2",6),F(19,"Our Core Values"),N(),T(20,"div",11)(21,"div",12)(22,"div",13),F(23,"\u{1F4A1}"),N(),T(24,"h3",14),F(25,"Innovation"),N(),T(26,"p",15),F(27,"We constantly push boundaries and explore new possibilities to deliver cutting-edge solutions."),N()(),T(28,"div",12)(29,"div",13),F(30,"\u{1F91D}"),N(),T(31,"h3",14),F(32,"Partnership"),N(),T(33,"p",15),F(34,"We build long-term relationships with our clients, working together to achieve shared goals."),N()(),T(35,"div",12)(36,"div",13),F(37,"\u2B50"),N(),T(38,"h3",14),F(39,"Excellence"),N(),T(40,"p",15),F(41,"We maintain the highest standards in everything we do, from code quality to client service."),N()()()(),T(42,"section",16)(43,"h2",6),F(44,"Our Leadership Team"),N(),T(45,"div",17)(46,"div",18),Oe(47,"img",19),T(48,"div",20)(49,"h3",21),F(50,"John Anderson"),N(),T(51,"p",22),F(52,"Chief Executive Officer"),N(),T(53,"p",23),F(54,"20+ years of experience in technology leadership and business strategy."),N()()(),T(55,"div",18),Oe(56,"img",24),T(57,"div",20)(58,"h3",21),F(59,"Sarah Chen"),N(),T(60,"p",22),F(61,"Chief Technology Officer"),N(),T(62,"p",23),F(63,"Expert in emerging technologies and digital transformation."),N()()(),T(64,"div",18),Oe(65,"img",25),T(66,"div",20)(67,"h3",21),F(68,"Michael Rodriguez"),N(),T(69,"p",22),F(70,"Chief Operations Officer"),N(),T(71,"p",23),F(72,"Specializes in optimizing business processes and team management."),N()()()()()()),r&2&&(_l(7),As("routerLink",Xl(1,y0)))},dependencies:[df],styles:[`[_nghost-%COMP%] {
    font-family: 'Inter', sans-serif;
    box-sizing: border-box;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
  }

  .hero-section[_ngcontent-%COMP%] {
    background: linear-gradient(rgba(0, 0, 0, 0.7), rgba(0, 0, 0, 0.7)),
                url('https://images.unsplash.com/photo-1497366216548-37526070297c?ixlib=rb-4.0.3');
    background-size: cover;
    background-position: center;
    height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    color: white;
    text-align: center;
  }

  .hero-content[_ngcontent-%COMP%] {
    max-width: 800px;
    padding: 2rem;
  }

  .hero-title[_ngcontent-%COMP%] {
    font-size: 4rem;
    font-weight: 700;
    margin-bottom: 1rem;
    text-transform: uppercase;
    letter-spacing: 2px;
  }

  .hero-subtitle[_ngcontent-%COMP%] {
    font-size: 1.5rem;
    margin-bottom: 2rem;
    font-weight: 300;
    line-height: 1.6;
  }

  .cta-button[_ngcontent-%COMP%] {
    background-color: #2C3E50;
    color: white;
    padding: 1rem 2rem;
    border: none;
    border-radius: 5px;
    font-size: 1.2rem;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.3s ease;
  }

  .cta-button[_ngcontent-%COMP%]:hover {
    background-color: #34495E;
    transform: translateY(-2px);
  }

  .about-section[_ngcontent-%COMP%] {
    padding: 5rem 2rem;
    background-color: #f8f9fa;
  }

  .section-title[_ngcontent-%COMP%] {
    text-align: center;
    font-size: 2.5rem;
    margin-bottom: 3rem;
    color: #2C3E50;
  }

  .about-content[_ngcontent-%COMP%] {
    max-width: 1200px;
    margin: 0 auto;
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 4rem;
    align-items: center;
  }

  .about-text[_ngcontent-%COMP%] {
    font-size: 1.1rem;
    line-height: 1.8;
    color: #555;
  }

  .about-image[_ngcontent-%COMP%] {
    width: 100%;
    border-radius: 10px;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
  }

  .values-section[_ngcontent-%COMP%] {
    padding: 5rem 2rem;
    background-color: white;
  }

  .values-grid[_ngcontent-%COMP%] {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 2rem;
    max-width: 1200px;
    margin: 0 auto;
  }

  .value-card[_ngcontent-%COMP%] {
    background: white;
    padding: 2rem;
    border-radius: 10px;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    text-align: center;
    transition: transform 0.3s ease;
  }

  .value-card[_ngcontent-%COMP%]:hover {
    transform: translateY(-5px);
  }

  .value-icon[_ngcontent-%COMP%] {
    font-size: 2.5rem;
    color: #2C3E50;
    margin-bottom: 1rem;
  }

  .value-title[_ngcontent-%COMP%] {
    font-size: 1.5rem;
    font-weight: 600;
    margin-bottom: 1rem;
    color: #2C3E50;
  }

  .value-description[_ngcontent-%COMP%] {
    color: #666;
    line-height: 1.6;
  }

  .team-section[_ngcontent-%COMP%] {
    padding: 5rem 2rem;
    background-color: #f8f9fa;
  }

  .team-grid[_ngcontent-%COMP%] {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 2rem;
    max-width: 1200px;
    margin: 0 auto;
  }

  .team-card[_ngcontent-%COMP%] {
    background: white;
    border-radius: 10px;
    overflow: hidden;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
  }

  .team-image[_ngcontent-%COMP%] {
    width: 100%;
    height: 300px;
    object-fit: cover;
  }

  .team-info[_ngcontent-%COMP%] {
    padding: 1.5rem;
    text-align: center;
  }

  .team-name[_ngcontent-%COMP%] {
    font-size: 1.5rem;
    font-weight: 600;
    margin-bottom: 0.5rem;
    color: #2C3E50;
  }

  .team-position[_ngcontent-%COMP%] {
    color: #666;
    font-size: 1rem;
    margin-bottom: 1rem;
  }

  .team-description[_ngcontent-%COMP%] {
    color: #666;
    line-height: 1.6;
  }

  @media (max-width: 768px) {
    .hero-title[_ngcontent-%COMP%] {
      font-size: 2.5rem;
    }
    
    .hero-subtitle[_ngcontent-%COMP%] {
      font-size: 1.2rem;
    }

    .about-content[_ngcontent-%COMP%] {
      grid-template-columns: 1fr;
      gap: 2rem;
    }
  }`]})}return e})();var Ef=[{path:"",redirectTo:"home",pathMatch:"full"},{path:"home",component:pf},{path:"home/app-ads.txt",component:hf},{path:"ads.txt",component:Po}];var mf={providers:[id({eventCoalescing:!0}),ff(Ef)]};Id(Po,mf).catch(e=>console.error(e));
