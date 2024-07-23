/* eslint-disable jsx-a11y/anchor-is-valid */
import * as React from "react";
import {
  faExclamationCircle,
  faHeadphones,
} from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { IconProp } from "@fortawesome/fontawesome-svg-core";
import { useLoaderData, Link, useNavigate, json } from "react-router-dom";
import { Helmet } from "react-helmet";
import tinycolor from "tinycolor2";
import { BarItemProps } from "@nivo/bar";
import { padStart } from "lodash";
import GlobalAppContext from "../../utils/GlobalAppContext";
import BrainzPlayer from "../../common/brainzplayer/BrainzPlayer";
import { getData, processData } from "./utils";

import Bar from "./components/Bar";
import Loader from "../../components/Loader";
import Pill from "../../components/Pill";
import {
  getAllStatRanges,
  getChartEntityDetails,
  getEntityLink,
  isInvalidStatRange,
  userChartEntityToListen,
} from "../stats/utils";
import ListenCard from "../../common/listens/ListenCard";
import {
  COLOR_LB_ASPHALT,
  COLOR_LB_BLUE,
  COLOR_LB_ORANGE,
} from "../../utils/constants";
import { getStatsArtistLink } from "../../utils/utils";
import { useMediaQuery } from "../../explore/fresh-releases/utils";
import ReleaseCard from "../../explore/fresh-releases/components/ReleaseCard";

export type UserEntityChartProps = {
  user?: ListenBrainzUser;
  entity: Entity;
  terminology: "artist" | "album" | "track";
  range: UserStatsAPIRange;
  currPage: number;
};

type UserEntityChartLoaderData = UserEntityChartProps;

export const TERMINOLOGY_ENTITY_MAP: Record<string, Entity> = {
  artist: "artist",
  album: "release-group",
  track: "recording",
};

const ROWS_PER_PAGE = 25;

// @ts-ignore - Not sure why it does not accept UserEntityDatum,
// but BarDatum does not represent the actual data format we have.
function CustomBarComponent(barProps: BarItemProps<UserEntityDatum>) {
  const { bar } = barProps;
  const { x, y, width, height, data } = bar;

  let title = `${data.data.count} listens | ${data.data.entity}`;
  if (data.data.artist) {
    title += ` - ${data.data.artist}`;
  }

  return (
    <g transform={`translate(${x}, ${y})`}>
      <rect
        width={width}
        height={height}
        fill={data.fill}
        strokeWidth="0"
        rx="0"
      />
      <foreignObject style={{ width: Math.max(170, width), height }}>
        <div className="graph-bar flex" title={title}>
          <div className="position">
            #{padStart(data.data.idx.toString(), 2, "0")}
          </div>
          <div className="graph-bar-text">
            <div className="graph-bar-entity ellipsis-2-lines">
              {getEntityLink(
                data.data.entityType,
                data.data.entity,
                data.data.entityMBID
              )}
            </div>
            {data.data.artist && (
              <div className="graph-bar-artist ellipsis">
                {getStatsArtistLink(
                  data.data.artists,
                  data.data.artist,
                  data.data.artistMBID
                )}
              </div>
            )}
          </div>
        </div>
      </foreignObject>
    </g>
  );
}

export default function UserEntityChart() {
  const loaderData = useLoaderData() as UserEntityChartLoaderData;
  const { user, entity, terminology, range, currPage } = loaderData;
  const prevPage = currPage - 1;
  const nextPage = currPage + 1;

  const { APIService, currentUser } = React.useContext(GlobalAppContext);
  const navigate = useNavigate();

  const [loading, setLoading] = React.useState(true);
  const [hasError, setHasError] = React.useState(false);
  const [errorMessage, setErrorMessage] = React.useState("");

  const [data, setData] = React.useState<UserEntityData>([]);
  const [maxListens, setMaxListens] = React.useState(0);
  const [totalPages, setTotalPages] = React.useState(0);
  const [entityCount, setEntityCount] = React.useState(0);
  const [startDate, setStartDate] = React.useState<Date | undefined>(undefined);
  const [endDate, setEndDate] = React.useState<Date | undefined>(undefined);
  const ranges = getAllStatRanges();

  const isMobile = useMediaQuery("(max-width: 767px)");

  React.useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setHasError(false);

      if (isInvalidStatRange(range)) {
        navigate(window.location.pathname);
        return;
      }

      try {
        const fetchedData = await getData(
          APIService,
          entity,
          currPage,
          range,
          ROWS_PER_PAGE,
          user
        );
        const entityData = processData(
          fetchedData.entityData,
          currPage,
          entity,
          ROWS_PER_PAGE
        );
        setData(entityData);
        setMaxListens(fetchedData.maxListens);
        setTotalPages(fetchedData.totalPages);
        setEntityCount(fetchedData.entityCount);
        setStartDate(fetchedData.startDate);
        setEndDate(fetchedData.endDate);
      } catch (error) {
        setHasError(true);
        setErrorMessage(error.message);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [APIService, currPage, entity, range, user, loaderData, navigate]);

  const listenContainer = React.useRef<HTMLDivElement>(null);

  const listenableItems: BaseListenFormat[] =
    data?.map(userChartEntityToListen) ?? [];

  const userOrLoggedInUser: string | undefined =
    user?.name ?? currentUser?.name;

  const userStatsTitle =
    user?.name === currentUser?.name ? "Your" : `${userOrLoggedInUser}'s`;

  const attributesForLinks = `?range=${range}`;
  return (
    <div role="main">
      <Helmet>
        <title>
          {user?.name ? userStatsTitle : "Sitewide"} top {terminology}s
        </title>
      </Helmet>
      <div style={{ marginTop: "1em", minHeight: 500 }}>
        <Loader isLoading={loading}>
          <div className="row">
            <div className="col-xs-12">
              <Pill active={terminology === "artist"} type="secondary">
                <Link
                  to={`../top-artists/${attributesForLinks}`}
                  relative="route"
                  className="user-charts-pill"
                >
                  Artists
                </Link>
              </Pill>
              <Pill active={terminology === "album"} type="secondary">
                <Link
                  to={`../top-albums/${attributesForLinks}`}
                  relative="route"
                  className="user-charts-pill"
                >
                  Albums
                </Link>
              </Pill>
              <Pill active={terminology === "track"} type="secondary">
                <Link
                  to={`../top-tracks/${attributesForLinks}`}
                  relative="route"
                  className="user-charts-pill"
                >
                  Tracks
                </Link>
              </Pill>
            </div>
          </div>
          <div className="row">
            <div className="col-xs-12">
              <h3>
                Top{" "}
                <span style={{ textTransform: "capitalize" }}>
                  {terminology ? `${terminology}s` : ""}
                </span>{" "}
                of {range !== "all_time" ? "the" : ""}
                <span className="dropdown" style={{ fontSize: 22 }}>
                  <button
                    className="dropdown-toggle btn-transparent capitalize-bold"
                    data-toggle="dropdown"
                    type="button"
                  >
                    {ranges.get(range)}
                    <span className="caret" />
                  </button>
                  <ul className="dropdown-menu" role="menu">
                    {Array.from(ranges, ([stat_type, stat_name]) => {
                      return (
                        <li key={`${stat_type}-${stat_name}`}>
                          <Link
                            to={{
                              pathname: window.location.pathname,
                              search: `?page=1&range=${stat_type}`,
                            }}
                            role="button"
                          >
                            {stat_name}
                          </Link>
                        </li>
                      );
                    })}
                  </ul>
                </span>
                {range !== "all_time" &&
                  !hasError &&
                  `(${startDate?.toLocaleString("en-us", {
                    day: "2-digit",
                    month: "long",
                    year: "numeric",
                  })} - ${endDate?.toLocaleString("en-us", {
                    day: "2-digit",
                    month: "long",
                    year: "numeric",
                  })})`}
              </h3>
            </div>
          </div>
          {hasError && (
            <div className="row mt-15 mb-15">
              <div className="col-xs-12 text-center">
                <span style={{ fontSize: 24 }}>
                  <FontAwesomeIcon icon={faExclamationCircle as IconProp} />{" "}
                  {errorMessage}
                </span>
              </div>
            </div>
          )}
          {!hasError && (
            <>
              <div className="row">
                <div className="col-xs-12">
                  <h4 style={{ textTransform: "capitalize" }}>
                    {terminology} count - <b>{entityCount}</b> / Page {currPage}
                  </h4>
                </div>
              </div>
              <div
                className="row bar-chart"
                style={{
                  minHeight: `calc(${Math.min(data.length, 25)} * 5em)`,
                }}
              >
                <Bar
                  isMobileSize={isMobile}
                  data={[...data].reverse()}
                  isInteractive={false}
                  maxValue={maxListens}
                  layout="horizontal"
                  enableGridX
                  barComponent={CustomBarComponent}
                  labelTextColor={COLOR_LB_ASPHALT}
                  margin={{
                    bottom: 40,
                    top: 40,
                    left: isMobile ? 5 : 15,
                    right: isMobile ? 30 : 15,
                  }}
                  defs={[
                    {
                      id: "barGradient",
                      type: "linearGradient",
                      colors: [
                        {
                          offset: 20,
                          color: tinycolor(COLOR_LB_BLUE)
                            .lighten(60)
                            .desaturate(15)
                            .toString(),
                        },
                        {
                          offset: 80,
                          color: tinycolor(COLOR_LB_ORANGE)
                            .spin(20)
                            .saturate(15)
                            .lighten(30)
                            .toString(),
                        },
                      ],
                      y2: "90vw",
                      gradientTransform: "rotate(-90)",
                      gradientUnits: "userSpaceOnUse",
                    },
                  ]}
                  fill={[{ match: "*", id: "barGradient" }]}
                  // labelPosition="start" // Upcoming nivo release, see https://github.com/plouc/nivo/pull/2585
                />
              </div>
              <div className="row">
                <div className="col-xs-12">
                  <ul className="pager">
                    <li
                      className={`previous ${
                        !(prevPage > 0) ? "disabled" : ""
                      }`}
                    >
                      <Link
                        to={{
                          pathname: window.location.pathname,
                          search: `?page=${prevPage}&range=${range}`,
                        }}
                        role="button"
                      >
                        &larr; Previous
                      </Link>
                    </li>
                    <li
                      className={`next ${
                        !(nextPage <= totalPages) ? "disabled" : ""
                      }`}
                    >
                      <Link
                        to={{
                          pathname: window.location.pathname,
                          search: `?page=${nextPage}&range=${range}`,
                        }}
                        role="button"
                      >
                        Next &rarr;
                      </Link>
                    </li>
                  </ul>
                </div>
              </div>

              {(entity === "artist" || entity === "recording") && (
                <div ref={listenContainer} className="top-entity-listencards">
                  {data?.slice().map((datum, index) => {
                    const listen = listenableItems[index];
                    const listenDetails = getChartEntityDetails(datum);
                    const listenCountComponent = (
                      <span className="badge badge-info">
                        {datum.count}
                        &nbsp;
                        <FontAwesomeIcon icon={faHeadphones} />
                      </span>
                    );
                    return (
                      <ListenCard
                        key={`${datum.idx + 1}`}
                        listenDetails={listenDetails}
                        listen={listen}
                        showTimestamp={false}
                        showUsername={false}
                        additionalActions={listenCountComponent}
                      />
                    );
                  })}
                </div>
              )}

              {(entity === "release" || entity === "release-group") && (
                <>
                  <p className="small">
                    <sup>*</sup>The listen count denotes the number of times you
                    have listened to a recording from the release group.
                  </p>
                  <div
                    ref={listenContainer}
                    className="release-cards-grid top-entity-grid"
                  >
                    {data?.slice().map((datum, index) => {
                      return (
                        <ReleaseCard
                          key={datum.entity + datum.entityMBID}
                          releaseName={datum.entity}
                          releaseGroupMBID={
                            entity === "release-group"
                              ? datum.entityMBID
                              : datum.releaseGroupMBID
                          }
                          releaseMBID={datum.releaseMBID}
                          artistMBIDs={
                            datum.artistMBID ??
                            datum.artists?.map((a) => a.artist_mbid) ??
                            []
                          }
                          artistCredits={datum.artists}
                          artistCreditName={datum.artist as string}
                          listenCount={datum.count}
                          caaID={datum.caaID ?? null}
                          caaReleaseMBID={datum.caaReleaseMBID ?? null}
                          showListens
                          showReleaseTitle
                          showArtist
                        />
                      );
                    })}
                  </div>
                </>
              )}
            </>
          )}
        </Loader>
      </div>

      <BrainzPlayer
        listens={listenableItems}
        listenBrainzAPIBaseURI={APIService.APIBaseURI}
        refreshSpotifyToken={APIService.refreshSpotifyToken}
        refreshYoutubeToken={APIService.refreshYoutubeToken}
        refreshSoundcloudToken={APIService.refreshSoundcloudToken}
      />
    </div>
  );
}

export const UserEntityChartLoader = async ({
  request,
}: {
  request: Request;
}) => {
  const currentURL = new URL(request.url);
  const response = await fetch(currentURL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
  });
  const propsData = await response.json();
  if (!response.ok) {
    throw json(propsData, { status: response.status });
  }
  const { user } = propsData;

  const page = Math.max(Number(currentURL.searchParams.get("page")), 1);
  const range: UserStatsAPIRange =
    (currentURL.searchParams.get("range") as UserStatsAPIRange) ?? "all_time";

  const match = currentURL.pathname.match(
    /\/user\/.+\/stats\/top-(artist|album|track)s/
  );
  const urlEntityName = match?.[1] ?? "artist";
  const entity = TERMINOLOGY_ENTITY_MAP[urlEntityName];

  return {
    user,
    entity,
    terminology: urlEntityName,
    currPage: page,
    range,
  };
};

export const StatisticsChartLoader = async ({
  request,
}: {
  request: Request;
}) => {
  const currentURL = new URL(request.url);
  const page = Math.max(Number(currentURL.searchParams.get("page")), 1);
  const range: UserStatsAPIRange =
    (currentURL.searchParams.get("range") as UserStatsAPIRange) ?? "all_time";

  const match = currentURL.pathname.match(
    /\/statistics\/top-(artist|album|track)s/
  );
  const urlEntityName = match?.[1] ?? "artist";
  const entity = TERMINOLOGY_ENTITY_MAP[urlEntityName];

  return {
    user: undefined,
    entity,
    terminology: urlEntityName,
    currPage: page,
    range,
  };
};
