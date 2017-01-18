<template>
  <div id="app">
    <header class="menu">
        <h1>Oracle Of Bacon</h1>
    </header>
    <nav>
      <actor-search id="actor-search" :on-search="consultOracle"></actor-search>
    </nav>
    <content>
      <search-result id="result" :actor-name="actorName" :actor-selected="displayActorOverview"></search-result>
      <aside>
        <last-10-searches id="last-10-searches" class="box"></last-10-searches>
        <actor-overview :actor-name="selectedActorName">
      </aside>
    </content>
  </div>
</template>

<script>
import 'whatwg-fetch';
import ActorSearch from './components/ActorSearch';
import SearchResult from './components/SearchResult';
import Last10Searches from './components/Last10Searches';
import ActorOverview from './components/ActorOverview';

export default {
  name: 'app',
  data() {
    return {
      actorName: '',
      selectedActorName: '',
    };
  },
  components: {
    ActorSearch,
    SearchResult,
    Last10Searches,
    ActorOverview,
  },
  methods: {
    consultOracle(actorName) {
      this.actorName = actorName;
    },
    displayActorOverview(actorName) {
      this.selectedActorName = actorName;
    },
  },
};
</script>

<style>
  h3, p, ol {
    text-align: center;
    font-family: 'Avenir', Helvetica, Arial, sans-serif;
    color: #37403F;
  }

  h1 {
    text-align: left;
    font-family: 'Avenir', Helvetica, Arial, sans-serif;
    color: #bbcba4;
    padding-left: 10px;
  }

  .menu {
    background-color: #5A847E;
    color: #fbffc3;
    position: absolute;
    width: 100%;
    margin: 0px Opx Opx 1Opx;
    top: 0px;
    right: 0px;
  }

  :root {
    background: url(./assets/kevin-bacon-background-left.png) no-repeat;
    -webkit-background-size: cover;
    -moz-background-size: cover;
    -o-background-size: cover;
    background-size: cover;
  }

  #last-10-searches {
    background-color: #fbf3c2;
    padding: 3em;
  }

  #connected-user {
    position: absolute;
    top: 20px;
    right: 20px;
  }

  *:focus {
    outline: none;
  }

  body {
    margin-top: 120px;
  }

  #app, aside {
    display: flex;
    flex-flow: column end;
  }
  
  :root {
    min-height: 100%;  
  }
  
  aside {
    max-width: 25em;
    background: blue;
  }
</style>
